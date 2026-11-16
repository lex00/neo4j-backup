"""Dagster resources — thin `ConfigurableResource` wrappers that delegate to the
orchestrator-agnostic clients in `neo4j_backup_core.clients`. The logic lives in core;
these just carry config and forward.
"""

from __future__ import annotations

import dagster as dg

from neo4j_backup_core.clients import BackupRunner, Neo4jClient, ObjectStore, object_store


_CLIENTS: dict = {}  # Neo4jResource configuration -> its shared client (#230)


class Neo4jResource(dg.ConfigurableResource):
    uri: str = "neo4j://localhost:7687"
    user: str = "neo4j"
    # Credential resolution (#18): `secret_provider` selects the backend (env | aws-sm),
    # `password_ref` is the provider-specific reference (env var name, or secret id/ARN).
    # `password` is an optional explicit override (e.g. dg.EnvVar) that wins when set — kept
    # for back-compat. Resolution is lazy (per connect) unless an explicit string is given.
    secret_provider: str = "env"
    password_ref: str | None = None
    password: str | None = None

    def _core(self) -> Neo4jClient:
        # one client (and so one driver) per configuration in this process (#230), not one per
        # statement; the client re-resolves the secret when it reconnects
        key = (self.uri, self.user, self.password, self.secret_provider, self.password_ref)
        client = _CLIENTS.get(key)
        if client is None:
            if self.password is not None:
                client = Neo4jClient(self.uri, self.user, self.password)
            else:
                from neo4j_backup_core import secrets

                provider = secrets.build(self.secret_provider)
                ref = self.password_ref
                client = Neo4jClient(self.uri, self.user, lambda: provider.resolve(ref))
            _CLIENTS[key] = client
        return client

    def close(self, *a, **k):
        return self._core().close(*a, **k)

    def _driver(self):
        return self._core()._driver()

    def run_on(self, *a, **k):
        return self._core().run_on(*a, **k)

    def run_system(self, *a, **k):
        return self._core().run_system(*a, **k)

    def seed_database(self, *a, **k):
        return self._core().seed_database(*a, **k)

    def alter_alias(self, *a, **k):
        return self._core().alter_alias(*a, **k)

    def create_alias(self, *a, **k):
        return self._core().create_alias(*a, **k)

    def alias_target(self, *a, **k):
        return self._core().alias_target(*a, **k)

    def resolve_physical(self, *a, **k):
        return self._core().resolve_physical(*a, **k)

    def database_exists(self, *a, **k):
        return self._core().database_exists(*a, **k)

    def database_created(self, *a, **k):
        return self._core().database_created(*a, **k)

    def database_access(self, *a, **k):
        return self._core().database_access(*a, **k)

    def set_read_only(self, *a, **k):
        return self._core().set_read_only(*a, **k)

    def stop_database(self, *a, **k):
        return self._core().stop_database(*a, **k)

    def drop_database(self, *a, **k):
        return self._core().drop_database(*a, **k)

    def count_nodes(self, *a, **k):
        return self._core().count_nodes(*a, **k)

    def list_databases(self, *a, **k):
        return self._core().list_databases(*a, **k)

    def list_aliases(self, *a, **k):
        return self._core().list_aliases(*a, **k)

    def _pol(self):
        """The policy, so the core's fence check knows the group's cluster (#217): without it
        Dagster backed up a group that `restore --to-cluster` had moved off this cluster."""
        from neo4j_backup_core.env import Neo4jSet, policy_path

        return Neo4jSet(self._core(), policy_source=policy_path())._pol()

    def for_group(self, group_id: str):
        """The client for the group's cluster (#106); this resource is the `default` cluster."""
        from neo4j_backup_core.env import Neo4jSet, policy_path

        return Neo4jSet(self._core(), policy_source=policy_path()).for_group(group_id)

    def privilege_commands(self, *a, **k):
        return self._core().privilege_commands(*a, **k)

    def database_online(self, *a, **k):
        return self._core().database_online(*a, **k)


class ObjectStoreResource(dg.ConfigurableResource):
    bucket: str
    endpoint_url: str | None = None
    region: str = "us-east-1"
    cloud: str | None = None  # aws (default) | azure ; gcp later (#52)
    # Encryption / extra args for the pipeline's boto3 PUT/COPY (buckets that require an
    # explicit SSE header on PutObject). Unset -> bucket default encryption.
    sse: str | None = None
    sse_kms_key_id: str | None = None
    write_args_json: str = "{}"

    def _core(self) -> ObjectStore:
        return object_store(self.bucket, self.endpoint_url, self.region,
                            self.sse, self.sse_kms_key_id, self.write_args_json, self.cloud)

    def _pol(self):
        from neo4j_backup_core.env import StoreSet, policy_path

        return StoreSet(self._core(), policy_source=policy_path())._pol()

    def for_group(self, group_id: str) -> ObjectStore:
        """The store a group's policy names (#105); this resource is the `default` store unless
        the policy defines one."""
        from neo4j_backup_core.env import StoreSet, policy_path

        return StoreSet(self._core(), policy_source=policy_path()).for_group(group_id)

    def list_artifacts(self, *a, **k):
        return self._core().list_artifacts(*a, **k)

    def latest_artifact_key(self, *a, **k):
        return self._core().latest_artifact_key(*a, **k)

    def object_size(self, *a, **k):
        return self._core().object_size(*a, **k)

    def delete_keys(self, *a, **k):
        return self._core().delete_keys(*a, **k)

    def copy_prefix(self, *a, **k):
        return self._core().copy_prefix(*a, **k)

    def delete_prefix(self, *a, **k):
        return self._core().delete_prefix(*a, **k)

    def uri(self, *a, **k):
        return self._core().uri(*a, **k)

    def upload_file(self, *a, **k):
        return self._core().upload_file(*a, **k)

    def upload_backups(self, *a, **k):
        return self._core().upload_backups(*a, **k)

    def download_prefix(self, *a, **k):
        return self._core().download_prefix(*a, **k)

    def sync_up(self, *a, **k):
        return self._core().sync_up(*a, **k)

    def put_text(self, *a, **k):
        return self._core().put_text(*a, **k)

    def get_text(self, *a, **k):
        return self._core().get_text(*a, **k)

    def list_text_keys(self, *a, **k):
        return self._core().list_text_keys(*a, **k)

    def create_exclusive(self, *a, **k):
        return self._core().create_exclusive(*a, **k)

    def list_load_artifacts(self, *a, **k):
        return self._core().list_load_artifacts(*a, **k)

    def list_prefixes(self, *a, **k):
        return self._core().list_prefixes(*a, **k)

    def latest_text_key(self, *a, **k):
        return self._core().latest_text_key(*a, **k)


class RunnerResource(dg.ConfigurableResource):
    backup_source: str = "neo4j:6362"
    scratch_path: str = "/scratch"
    pagecache: str = "512M"
    heap_size: str = "2G"
    neo4j_admin: str = "neo4j-admin"
    exec_prefix: list[str] = []
    mode: str = "subprocess"
    image: str = ""
    node_selector_json: str = "{}"
    memory_limit: str = "4Gi"
    scratch_storage: str = "6Ti"
    service_account: str = ""
    extra_env_json: str = "{}"
    namespace: str = ""            # k8s (#229): "" = the run pod's namespace
    cpu_request: str = ""
    memory_request: str = ""
    tolerations_json: str = "[]"
    secret_env_json: str = "{}"    # {"ENV": "<secret>:<key>"}: credentials from k8s Secrets

    def _core(self) -> BackupRunner:
        return BackupRunner(
            self.backup_source, self.scratch_path, self.pagecache, self.heap_size,
            self.neo4j_admin, self.exec_prefix, self.mode, self.image,
            self.node_selector_json, self.memory_limit, self.scratch_storage,
            self.service_account, self.extra_env_json,
            namespace=self.namespace, cpu_request=self.cpu_request,
            memory_request=self.memory_request, tolerations_json=self.tolerations_json,
            secret_env_json=self.secret_env_json,
        )

    def backup_command(self, *a, **k):
        return self._core().backup_command(*a, **k)

    def aggregate_command(self, *a, **k):
        return self._core().aggregate_command(*a, **k)

    def check_command(self, *a, **k):
        return self._core().check_command(*a, **k)

    def for_group(self, group_id: str):
        """The runner for the group's cluster (its `backup_source`, #106)."""
        from neo4j_backup_core.env import RunnerSet, policy_path

        return RunnerSet(self._core(), policy_source=policy_path()).for_group(group_id)

    def with_env(self, *a, **k):
        return self._core().with_env(*a, **k)

    def inspect_command(self, *a, **k):
        return self._core().inspect_command(*a, **k)

    def dump_command(self, *a, **k):
        return self._core().dump_command(*a, **k)

    def import_dump_command(self, *a, **k):
        return self._core().import_dump_command(*a, **k)

    def command_build_command(self, *a, **k):
        return self._core().command_build_command(*a, **k)

    def export_command(self, *a, **k):
        return self._core().export_command(*a, **k)

    def import_command(self, *a, **k):
        return self._core().import_command(*a, **k)

    def env(self, *a, **k):
        return self._core().env(*a, **k)

    def pod_env(self, *a, **k):
        return self._core().pod_env(*a, **k)

    def k8s_resources(self, *a, **k):
        return self._core().k8s_resources(*a, **k)

    def k8s_pod_spec(self, *a, **k):
        return self._core().k8s_pod_spec(*a, **k)
