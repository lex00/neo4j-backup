"""Build the core clients from the runtime environment — shared by the Airflow adapter and the CLI
(the Dagster adapter carries the same wiring in its `ConfigurableResource`s instead). Imports no
orchestrator; the same env table documented in orchestrator/README applies.
"""

from __future__ import annotations

import json
import os

from . import secrets
from .capture import run_captured
from .clients import BackupRunner, Neo4jClient, ObjectStore, object_store


def subprocess_admin(runner, stdout=None):
    """A `run_admin(cmd) -> str` callable for `ops.*` that runs one neo4j-admin command as a local
    subprocess with the runner's environment (raises on non-zero) and returns its output (#118).
    Shared by the CLI and the MCP server; both pass `stdout=sys.stderr` so the child's output never
    lands on their own stdout (the CLI's JSON envelope / the MCP stdio protocol)."""
    def run(cmd, env=None):
        extra = env or {}
        return run_captured(runner.with_env(cmd, extra), env={**runner.env(), **extra},
                            stream=stdout)
    return run


def policy_path() -> str:
    return os.environ.get("NEO4J_BACKUP_POLICY", "policies/demo.yaml")


def env_neo4j() -> Neo4jClient:
    # Credential via a secret provider (#18), resolved lazily per connect. Default
    # SECRET_PROVIDER=env reads NEO4J_PASSWORD; aws-sm uses NEO4J_PASSWORD_REF (secret id/ARN).
    provider = secrets.from_env()
    ref = os.environ.get("NEO4J_PASSWORD_REF")
    return Neo4jClient(
        os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"),
        os.environ.get("NEO4J_USER", "neo4j"),
        lambda: provider.resolve(ref),
    )


def env_store() -> ObjectStore:
    """The store the environment describes (BACKUP_BUCKET, CLOUD, …): `default` when the policy
    names no `default` store (#105)."""
    return object_store(
        os.environ.get("BACKUP_BUCKET", "neo4j-backups"),
        os.environ.get("AWS_ENDPOINT_URL_S3") or None,  # unset on real AWS
        os.environ.get("AWS_REGION", "us-east-1"),
        sse=os.environ.get("S3_SSE") or None,
        sse_kms_key_id=os.environ.get("S3_SSE_KMS_KEY_ID") or None,
        write_args_json=os.environ.get("S3_WRITE_ARGS", "{}"),
        cloud=os.environ.get("CLOUD") or None,  # aws (default) | azure | gcp
    )


def store_from_config(cfg) -> ObjectStore:
    """Build an object store from a policy `storage:` entry (#105), with its own credentials and
    the environment neo4j-admin needs for it (`admin_env()`)."""
    if cfg.cloud == "azure" and cfg.account:
        os.environ.setdefault("AZURE_STORAGE_ACCOUNT", cfg.account)
    region = cfg.region or os.environ.get("AWS_REGION", "us-east-1")
    st = object_store(cfg.bucket, cfg.endpoint, region, sse=cfg.sse,
                      sse_kms_key_id=cfg.sse_kms_key_id,
                      write_args_json=json.dumps(cfg.write_args or {}), cloud=cfg.cloud)
    if cfg.cloud != "aws":
        if cfg.credentials_ref:
            raise RuntimeError(f"store {cfg.url!r}: credentials_ref is supported for s3:// "
                               f"stores only so far; use the environment's credentials")
        return st
    admin = {}
    if cfg.region:
        admin["AWS_REGION"] = cfg.region
    if cfg.runner_endpoint or cfg.endpoint:
        admin["AWS_ENDPOINT_URL_S3"] = cfg.runner_endpoint or cfg.endpoint
    if cfg.credentials_ref:
        st.credentials = secrets.key_pair(cfg.credentials_ref)
        admin["AWS_ACCESS_KEY_ID"] = st.credentials["access_key_id"]
        admin["AWS_SECRET_ACCESS_KEY"] = st.credentials["secret_access_key"]
        if st.credentials.get("session_token"):
            admin["AWS_SESSION_TOKEN"] = st.credentials["session_token"]
    st.admin_env_vars = admin
    return st


class StoreSet:
    """The stores a deployment uses (#105). Acts as the `default` store (every ObjectStore method
    is forwarded to it), and `for_group(group_id)` returns the store a group's policy names. The
    core ops call `for_group` wherever they know the group, so callers pass one StoreSet."""

    def __init__(self, default: ObjectStore, policy_source=None, policy=None):
        self._default, self._source, self._policy = default, policy_source, policy
        self._built: dict[str, ObjectStore] = {}

    def _pol(self):
        if self._policy is not None:
            return self._policy
        from .policy import load_policy
        return load_policy(self._source)

    def named(self, name: str) -> ObjectStore:
        if name not in self._built:
            try:
                pol = self._pol() if (self._policy is not None or self._source) else None
            except Exception:  # noqa: BLE001 - no loadable policy: only the env default exists
                pol = None
            cfg = pol.storage.get(name) if pol is not None else None
            self._built[name] = store_from_config(cfg) if cfg else (
                self._default if name == "default" else None)
            if self._built[name] is None:
                raise KeyError(f"no store named {name!r}")
        return self._built[name]

    def for_group(self, group_id: str) -> ObjectStore:
        try:
            name = self._pol().store_name(group_id)
        except KeyError:  # not a policy group (e.g. a probe): the default store
            name = "default"
        return self.named(name)

    def __getattr__(self, attr):
        return getattr(self.named("default"), attr)


def check_stores(policy, stores: "StoreSet") -> list[dict]:
    """Can the tool list and write each store the policy uses (#105)? Writes and deletes one
    probe object per store under `_policy_check/`. Also says which stores the Neo4j servers must
    read (they fetch seeds with their own cloud configuration) and which settings won't reach
    them."""
    import time

    used: dict[str, list[str]] = {}
    for g in policy.db_groups:
        used.setdefault(policy.store_name(g.id), []).append(g.id)
        if g.load.source and g.load.source.storage:
            used.setdefault(g.load.source.storage, [])
    used.setdefault("default", [])
    out = []
    for name in sorted(used):
        row = {"store": name, "groups": used[name], "list": False, "write": False, "errors": []}
        cfg = policy.storage.get(name)
        row["url"] = cfg.url if cfg else "(environment)"
        try:
            st = stores.named(name)
            st.list_text_keys("_policy_check/", ".txt")
            row["list"] = True
            key = f"_policy_check/{os.uname().nodename}-{int(time.time())}.txt"
            st.put_text(key, "neo4j-backup policy check")
            st.delete_keys([key])
            row["write"] = True
        except Exception as e:  # noqa: BLE001 - report every store, don't stop at the first
            row["errors"].append(f"{type(e).__name__}: {e}")
        if used[name]:
            row["servers"] = ("every Neo4j cluster serving these groups must be able to read "
                              "this store: seeding is done by the servers with their own cloud "
                              "configuration")
        if cfg and (cfg.endpoint or cfg.credentials_ref or cfg.region):
            row["servers_note"] = ("this store's region, endpoint and credentials reach the tool "
                                   "and neo4j-admin, not the Neo4j servers")
        out.append(row)
    return out


def store() -> StoreSet:
    """The deployment's stores: the `default` store (policy `storage.default`, else the
    environment) plus each group's named store, resolved per group (#105)."""
    return StoreSet(env_store(), policy_source=policy_path())


def env_runner(exec_prefix=None) -> BackupRunner:
    return BackupRunner(
        backup_source=os.environ.get("NEO4J_BACKUP_SOURCE", "neo4j:6362"),
        scratch_path=os.environ.get("SCRATCH_PATH", "/scratch"),
        pagecache=os.environ.get("RUNNER_PAGECACHE", "512M"),
        heap_size=os.environ.get("RUNNER_HEAP_SIZE", "2G"),
        neo4j_admin=os.environ.get("RUNNER_NEO4J_ADMIN", "neo4j-admin"),
        exec_prefix=(
            exec_prefix if exec_prefix is not None
            else json.loads(os.environ.get("RUNNER_EXEC_PREFIX", "[]"))
        ),
        mode=os.environ.get("RUNNER_MODE", "subprocess"),
        image=os.environ.get("RUNNER_IMAGE", ""),
        node_selector_json=os.environ.get("RUNNER_NODE_SELECTOR", "{}"),
        memory_limit=os.environ.get("RUNNER_MEMORY_LIMIT", "4Gi"),
        scratch_storage=os.environ.get("RUNNER_SCRATCH_STORAGE", "6Ti"),
        service_account=os.environ.get("RUNNER_SERVICE_ACCOUNT", ""),
        extra_env_json=os.environ.get("RUNNER_EXTRA_ENV", "{}"),
    )


# --- clusters (#106) --------------------------------------------------------------------------

class _PerGroup:
    """Acts as the `default` cluster's handle (attributes forwarded) and gives each group its
    cluster's handle with `for_group`. Shared by Neo4jSet and RunnerSet."""

    def __init__(self, default, policy_source=None, policy=None):
        self._default, self._source, self._policy = default, policy_source, policy
        self._built: dict[str, object] = {}

    def _pol(self):
        if self._policy is not None:
            return self._policy
        if not self._source:
            return None
        from .policy import load_policy
        try:
            return load_policy(self._source)
        except Exception:  # noqa: BLE001 - no loadable policy: only the env default exists
            return None

    def named(self, name: str):
        if name not in self._built:
            pol = self._pol()
            cfg = pol.clusters.get(name) if pol is not None else None
            self._built[name] = self._build(cfg) if cfg else self._default
        return self._built[name]

    def for_group(self, group_id: str):
        pol = self._pol()
        try:
            name = pol.cluster_name(group_id) if pol is not None else "default"
        except KeyError:
            name = "default"
        return self.named(name)

    def __getattr__(self, attr):
        return getattr(self.named("default"), attr)


def _cluster_attrs(obj, cfg):
    obj.cluster_topology = cfg.topology
    obj.cypher_version = cfg.cypher_version
    obj.cutover_config = cfg.cutover
    obj.backup_upload = cfg.backup_upload
    return obj


class Neo4jSet(_PerGroup):
    def _build(self, cfg):
        ref = cfg.password_ref
        provider = secrets.from_env()
        pw = (lambda: secrets.resolve_ref(ref)) if ref else (
            lambda: provider.resolve(os.environ.get("NEO4J_PASSWORD_REF")))
        return _cluster_attrs(Neo4jClient(cfg.bolt_uri, cfg.user, pw), cfg)


class RunnerSet(_PerGroup):
    def _build(self, cfg):
        import copy

        r = copy.copy(self._default)
        r.backup_source = cfg.backup_sources
        return _cluster_attrs(r, cfg)


def neo4j():
    """The Neo4j client per cluster (#106): the `default` cluster (policy `clusters.default`,
    else the environment) plus each group's cluster, resolved per group."""
    return Neo4jSet(env_neo4j(), policy_source=policy_path())


def runner(exec_prefix=None):
    """The neo4j-admin runner per cluster (#106): each cluster's `backup_source`."""
    return RunnerSet(env_runner(exec_prefix), policy_source=policy_path())
