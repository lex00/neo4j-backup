"""Plain, framework-free clients: Bolt (restore Cypher), object store (boto3), and the
neo4j-admin runner (command building + k8s pod spec). The Dagster/Airflow adapters wrap
these; the logic lives here once.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Protocol, runtime_checkable

from .retry import retry_bolt


class Neo4jClient:
    """Bolt client for system-database operations: seed-from-URI and alias swap. The
    agentless restore surface — pure Cypher, nothing on the instance (DESIGN.md §3)."""

    def __init__(self, uri: str = "neo4j://localhost:7687", user: str = "neo4j",
                 password="") -> None:
        # `password` is a str OR a zero-arg callable resolving one (a secret provider, #18).
        # A callable is invoked per connect in _driver(), so a rotated secret is picked up on
        # the next connection.
        self.uri, self.user, self.password = uri, user, password

    @contextmanager
    def _driver(self):
        from neo4j import GraphDatabase

        pw = self.password() if callable(self.password) else self.password
        driver = GraphDatabase.driver(self.uri, auth=(self.user, pw))
        try:
            yield driver
        finally:
            driver.close()

    def run_on(self, database: str, cypher: str, **params):
        """Run Cypher against `database`, returning row dicts, with the transient-retry
        contract (retry.py / #19) and lazy credential resolution (#18). This is the single
        Bolt path — callers use it (or `run_system`) instead of opening raw sessions, so
        retry/credential handling is never bypassed (#24). Each retry rebuilds the driver via
        _driver(), which re-resolves the credential, so no explicit on_auth_expired is needed.
        """
        def _op():
            with self._driver() as d, d.session(database=database) as s:
                return [r.data() for r in s.run(cypher, **params)]
        return retry_bolt(_op)

    def run_system(self, cypher: str, **params):
        return self.run_on("system", cypher, **params)

    # CloudSeedProvider takes region/endpoint from server env; no seedConfig (rejected).
    # `existingData: 'use'` with seedURI is REQUIRED in Cypher 5 and DEPRECATED in Cypher 25
    # (Neo4j docs), so it is coupled to the pinned language version: cypher_version=None
    # (default) emits no CYPHER prefix and omits existingData — the validated behavior on a
    # Cypher-25 cluster (2025+); set cypher_version="5" on a Cypher-5 cluster.
    # `topology` (any object exposing .primaries/.secondaries — e.g. policy.Topology) adds
    # the clustered `TOPOLOGY n PRIMARIES m SECONDARIES` clause; omit for standalone.
    def seed_database(self, name: str, seed_uri: str, restore_until: str | None = None,
                      topology=None, cypher_version: str | None = None):
        prefix = f"CYPHER {cypher_version} " if cypher_version else ""
        topo = ""
        if topology is not None:
            topo = (f" TOPOLOGY {topology.primaries} PRIMARIES "
                    f"{topology.secondaries} SECONDARIES")
        opts = f"seedURI: '{seed_uri}'"
        if cypher_version == "5":
            opts += ", existingData: 'use'"
        if restore_until:
            opts += f", seedRestoreUntil: datetime('{restore_until}')"
        self.run_system(f"{prefix}CREATE DATABASE `{name}`{topo} OPTIONS {{ {opts} }} WAIT")

    def alter_alias(self, alias: str, target: str):
        self.run_system(f"ALTER ALIAS `{alias}` SET DATABASE TARGET `{target}`")

    def create_alias(self, alias: str, target: str):
        self.run_system(f"CREATE ALIAS `{alias}` IF NOT EXISTS FOR DATABASE `{target}`")

    def alias_target(self, alias: str) -> str | None:
        rows = self.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name, database "
            "WHERE name = $a RETURN database",
            a=alias,
        )
        return rows[0]["database"] if rows else None

    def database_exists(self, name: str) -> bool:
        rows = self.run_system(
            "SHOW DATABASES YIELD name WHERE name = $n RETURN name", n=name
        )
        return bool(rows)

    def resolve_physical(self, name: str) -> str | None:
        """Resolve `name` to a physical database: an alias -> its current target; an existing
        database -> itself; otherwise None. Lets callers accept either an alias or a physical
        name instead of assuming an alias."""
        target = self.alias_target(name)
        if target:
            return target
        return name if self.database_exists(name) else None

    def stop_database(self, name: str):
        self.run_system(f"STOP DATABASE `{name}` WAIT")

    def drop_database(self, name: str):
        self.run_system(f"DROP DATABASE `{name}` IF EXISTS WAIT")

    def count_nodes(self, database: str) -> int:
        return self.run_on(database, "MATCH (n) RETURN count(n) AS n")[0]["n"]

    def list_databases(self) -> list[str]:
        return [r["name"] for r in self.run_system("SHOW DATABASES YIELD name")]

    def list_aliases(self) -> dict[str, str]:
        rows = self.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name, database RETURN name, database"
        )
        return {r["name"]: r["database"] for r in rows}


@runtime_checkable
class ObjectStore(Protocol):
    """The object-store interface the pipeline depends on — one shape, per-cloud backends
    (S3 today; Azure/GCP later, #52). Adapters obtain an instance from `object_store()`; the
    resource delegation guard (tests/test_resources.py) checks every method here is forwarded."""

    def list_artifacts(self, prefix: str): ...
    def latest_artifact_key(self, prefix: str) -> str | None: ...
    def object_size(self, key: str) -> int: ...
    def delete_keys(self, keys: list[str]) -> int: ...
    def copy_prefix(self, src_prefix: str, dst_prefix: str) -> int: ...
    def delete_prefix(self, prefix: str) -> int: ...
    def uri(self, key: str) -> str: ...
    def upload_file(self, local_path: str, key: str) -> str: ...
    def upload_backups(self, local_dir: str, prefix: str, cleanup: bool = True) -> str | None: ...
    def download_prefix(self, prefix: str, local_dir: str) -> int: ...
    def sync_up(self, local_dir: str, prefix: str, cleanup: bool = True) -> str | None: ...
    def put_text(self, key: str, text: str) -> str: ...
    def get_text(self, key: str) -> str: ...
    def list_text_keys(self, prefix: str, suffix: str = ".cypher"): ...
    def latest_text_key(self, prefix: str, suffix: str = ".cypher") -> str | None: ...


def object_store(bucket: str, endpoint_url: str | None = None, region: str = "us-east-1",
                 sse: str | None = None, sse_kms_key_id: str | None = None,
                 write_args_json: str = "{}", cloud: str | None = None) -> ObjectStore:
    """Build the object-store backend for `cloud` (aws (default) | azure; gcp later, #52)."""
    if (cloud or "aws").lower() in ("azure", "az", "azb"):
        return AzureObjectStore(bucket, endpoint_url, region, sse, sse_kms_key_id, write_args_json)
    return S3ObjectStore(bucket, endpoint_url, region, sse, sse_kms_key_id, write_args_json)


class _BaseObjectStore:
    """Cloud-agnostic composites (chains, prefixes, local<->store transfers) built on the
    per-backend primitives. A backend subclass implements: `list_artifacts`, `object_size`,
    `delete_keys`, `_copy`, `_download`, `upload_file`, `put_text`, `get_text`, `list_text_keys`,
    `uri`, and carries a `write_args` dict."""

    write_args: dict

    def latest_artifact_key(self, prefix: str) -> str | None:
        arts = self.list_artifacts(prefix)
        return max(arts, key=lambda t: t[2])[0] if arts else None

    def delete_prefix(self, prefix: str) -> int:
        return self.delete_keys([k for (k, _s, _m) in self.list_artifacts(prefix)])

    def copy_prefix(self, src_prefix: str, dst_prefix: str) -> int:
        n = 0
        for key, _s, _m in self.list_artifacts(src_prefix):
            self._copy(key, dst_prefix + key[len(src_prefix):])
            n += 1
        return n

    def download_prefix(self, prefix: str, local_dir: str) -> int:
        import os

        os.makedirs(local_dir, exist_ok=True)
        n = 0
        for key, _s, _m in self.list_artifacts(prefix):
            self._download(key, os.path.join(local_dir, key[len(prefix):]))
            n += 1
        return n

    def upload_backups(self, local_dir: str, prefix: str, cleanup: bool = True) -> str | None:
        """Upload every `*.backup` in `local_dir` to `prefix`, return the newest key, remove
        `local_dir`. The write leg of BACKUP_UPLOAD=pipeline."""
        import os
        import shutil

        latest = None
        for name in sorted(f for f in os.listdir(local_dir) if f.endswith(".backup")):
            latest = self.upload_file(os.path.join(local_dir, name), prefix + name)
        if cleanup:
            shutil.rmtree(local_dir, ignore_errors=True)
        return latest

    def sync_up(self, local_dir: str, prefix: str, cleanup: bool = True) -> str | None:
        """Make `prefix` match `local_dir`'s `.backup` set: upload each, delete artifacts no
        longer present (e.g. diffs collapsed by aggregate), remove `local_dir`. Newest key."""
        import os
        import shutil

        local = {f for f in os.listdir(local_dir) if f.endswith(".backup")}
        latest = None
        for name in sorted(local):
            latest = self.upload_file(os.path.join(local_dir, name), prefix + name)
        stale = [k for (k, _s, _m) in self.list_artifacts(prefix) if k[len(prefix):] not in local]
        self.delete_keys(stale)
        if cleanup:
            shutil.rmtree(local_dir, ignore_errors=True)
        return latest

    def latest_text_key(self, prefix: str, suffix: str = ".cypher") -> str | None:
        arts = self.list_text_keys(prefix, suffix)
        return max(arts, key=lambda t: t[1])[0] if arts else None


class S3ObjectStore(_BaseObjectStore):
    """S3-compatible object store (boto3).

    `write_args` are merged into every boto3 PUT/COPY the pipeline issues (put_text, copy_prefix).
    A bucket whose policy *requires* an explicit encryption header on PutObject (e.g. deny unless
    `x-amz-server-side-encryption = aws:kms`) needs this — otherwise the default is the bucket's
    own default encryption. Convenience: `sse`/`sse_kms_key_id` fill in ServerSideEncryption /
    SSEKMSKeyId; `write_args_json` is a JSON escape hatch for any other PUT/COPY arg
    (BucketKeyEnabled, ACL, …). Note: neo4j-admin's own `.backup` uploads are governed separately
    (its S3 config / the bucket default), not by this."""

    def __init__(self, bucket: str, endpoint_url: str | None = None,
                 region: str = "us-east-1", sse: str | None = None,
                 sse_kms_key_id: str | None = None, write_args_json: str = "{}"):
        self.bucket, self.endpoint_url, self.region = bucket, endpoint_url, region
        self.write_args: dict = json.loads(write_args_json or "{}")
        if sse:
            self.write_args["ServerSideEncryption"] = sse
        if sse_kms_key_id:
            self.write_args["SSEKMSKeyId"] = sse_kms_key_id

    def _client(self):
        import boto3
        from botocore.config import Config

        cfg = Config(s3={"addressing_style": "path"}) if self.endpoint_url else None
        return boto3.client(
            "s3", endpoint_url=self.endpoint_url, region_name=self.region, config=cfg
        )

    def list_artifacts(self, prefix: str):
        out = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".backup"):
                    out.append((o["Key"], o["Size"], o["LastModified"]))
        return out

    def object_size(self, key: str) -> int:
        return self._client().head_object(Bucket=self.bucket, Key=key)["ContentLength"]

    def delete_keys(self, keys: list[str]) -> int:
        if not keys:
            return 0
        self._client().delete_objects(
            Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )
        return len(keys)

    def _copy(self, src_key: str, dst_key: str) -> None:
        self._client().copy_object(
            Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src_key},
            Key=dst_key, **self.write_args,
        )

    def _download(self, key: str, local_path: str) -> None:
        self._client().download_file(self.bucket, key, local_path)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def upload_file(self, local_path: str, key: str) -> str:
        """Upload a local file to `key`, applying `write_args` (SSE-KMS, …). Managed transfer,
        so multi-GB artifacts multipart automatically and each part carries the encryption."""
        self._client().upload_file(local_path, self.bucket, key, ExtraArgs=self.write_args or None)
        return key

    # --- text artifacts (the logical metadata export; .cypher, not .backup) ---
    # Encryption follows `write_args`: unset -> the bucket's default encryption applies (as for
    # the neo4j-admin .backup writes); set S3_SSE/S3_SSE_KMS_KEY_ID to send an explicit header
    # for buckets that require one. The export carries no plaintext secrets (passwords aren't
    # exported), but it is encrypted at rest like everything else.
    def put_text(self, key: str, text: str) -> str:
        self._client().put_object(
            Bucket=self.bucket, Key=key, Body=text.encode(),
            ContentType="text/plain; charset=utf-8",
            **self.write_args,
        )
        return key

    def get_text(self, key: str) -> str:
        return self._client().get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()

    def list_text_keys(self, prefix: str, suffix: str = ".cypher"):
        out = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(suffix):
                    out.append((o["Key"], o["LastModified"]))
        return out


class AzureObjectStore(_BaseObjectStore):
    """Azure Blob Storage backend (#52). `bucket` is the **container**; the storage account,
    endpoint, and credential come from `AZURE_STORAGE_CONNECTION_STRING` — which works against
    **Azurite** and real Azure alike. `write_args` are passthrough kwargs to blob uploads (e.g.
    `encryption_scope`); the S3-only `sse`/`sse_kms_key_id` are ignored (Azure encrypts at the
    account/scope level). Neo4j's own seed/backup fetch uses `azb://` — see uri()."""

    def __init__(self, bucket: str, endpoint_url: str | None = None, region: str = "us-east-1",
                 sse: str | None = None, sse_kms_key_id: str | None = None,
                 write_args_json: str = "{}"):
        self.container = bucket
        self.write_args: dict = json.loads(write_args_json or "{}")

    def _c(self):
        from azure.storage.blob import BlobServiceClient

        conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        return BlobServiceClient.from_connection_string(conn).get_container_client(self.container)

    def list_artifacts(self, prefix: str):
        return [(b.name, b.size, b.last_modified)
                for b in self._c().list_blobs(name_starts_with=prefix) if b.name.endswith(".backup")]

    def object_size(self, key: str) -> int:
        return self._c().get_blob_client(key).get_blob_properties().size

    def delete_keys(self, keys: list[str]) -> int:
        c = self._c()
        for k in keys:
            c.delete_blob(k)
        return len(keys)

    def _copy(self, src_key: str, dst_key: str) -> None:
        c = self._c()
        c.get_blob_client(dst_key).start_copy_from_url(c.get_blob_client(src_key).url)

    def _download(self, key: str, local_path: str) -> None:
        with open(local_path, "wb") as f:
            f.write(self._c().get_blob_client(key).download_blob().readall())

    def uri(self, key: str) -> str:
        account = os.environ.get("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
        return f"azb://{account}/{self.container}/{key}"

    def upload_file(self, local_path: str, key: str) -> str:
        with open(local_path, "rb") as f:
            self._c().upload_blob(name=key, data=f, overwrite=True, **self.write_args)
        return key

    def put_text(self, key: str, text: str) -> str:
        from azure.storage.blob import ContentSettings

        self._c().upload_blob(
            name=key, data=text.encode(), overwrite=True,
            content_settings=ContentSettings(content_type="text/plain; charset=utf-8"),
            **self.write_args,
        )
        return key

    def get_text(self, key: str) -> str:
        return self._c().get_blob_client(key).download_blob().readall().decode()

    def list_text_keys(self, prefix: str, suffix: str = ".cypher"):
        return [(b.name, b.last_modified)
                for b in self._c().list_blobs(name_starts_with=prefix) if b.name.endswith(suffix)]


class BackupRunner:
    """neo4j-admin command building + the validated memory/scratch caps (DESIGN.md §5.5)
    and the k8s pod spec. Execution (subprocess vs pod) is the adapter's job."""

    def __init__(self, backup_source: str = "neo4j:6362", scratch_path: str = "/scratch",
                 pagecache: str = "512M", heap_size: str = "2G",
                 neo4j_admin: str = "neo4j-admin", exec_prefix: list | None = None,
                 mode: str = "subprocess", image: str = "", node_selector_json: str = "{}",
                 memory_limit: str = "4Gi", scratch_storage: str = "6Ti",
                 service_account: str = "", extra_env_json: str = "{}"):
        self.backup_source, self.scratch_path = backup_source, scratch_path
        self.pagecache, self.heap_size = pagecache, heap_size
        self.neo4j_admin, self.exec_prefix = neo4j_admin, list(exec_prefix or [])
        self.mode, self.image = mode, image
        self.node_selector_json, self.memory_limit = node_selector_json, memory_limit
        self.scratch_storage, self.service_account = scratch_storage, service_account
        self.extra_env_json = extra_env_json

    def backup_command(self, database: str, to_path: str, kind: str = "AUTO") -> list:
        return [
            *self.exec_prefix, self.neo4j_admin, "database", "backup",
            "--from", self.backup_source, "--to-path", to_path,
            "--temp-path", self.scratch_path, "--pagecache", self.pagecache,
            "--type", kind, "--compress=true", database,
        ]

    def aggregate_command(self, database: str, from_path: str, keep_old: bool = False) -> list:
        cmd = [
            *self.exec_prefix, self.neo4j_admin, "backup", "aggregate",
            f"--from-path={from_path}", f"--temp-path={self.scratch_path}",
        ]
        if keep_old:
            cmd.append("--keep-old-backup=true")
        cmd.append(database)
        return cmd

    def check_command(self, database: str, from_path: str, max_off_heap: str = "50%") -> list:
        return [
            *self.exec_prefix, self.neo4j_admin, "database", "check",
            f"--from-path={from_path}", f"--temp-path={self.scratch_path}",
            f"--max-off-heap-memory={max_off_heap}", database,
        ]

    def env(self) -> dict:
        return {"HEAP_SIZE": self.heap_size}

    def k8s_pod_spec(self, env: dict) -> dict:
        env = {**json.loads(self.extra_env_json or "{}"), **env}
        spec = {
            "containers": [{
                "name": "neo4j-admin",
                "imagePullPolicy": "IfNotPresent",
                "env": [{"name": k, "value": v} for k, v in env.items()],
                "resources": {"limits": {"memory": self.memory_limit}},
                "volumeMounts": [{"name": "scratch", "mountPath": self.scratch_path}],
            }],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {
                "spec": {"accessModes": ["ReadWriteOnce"],
                         "resources": {"requests": {"storage": self.scratch_storage}}}}}}],
        }
        ns = json.loads(self.node_selector_json or "{}")
        if ns:
            spec["nodeSelector"] = ns
        if self.service_account:
            spec["serviceAccountName"] = self.service_account
        return spec
