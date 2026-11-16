"""Plain, framework-free clients: Bolt (restore Cypher), object store (boto3), and the
neo4j-admin runner (command building + k8s pod spec). The Dagster/Airflow adapters wrap
these; the logic lives here once.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Protocol, runtime_checkable

from .cypher import quote, seed_uri_literal
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
                      topology=None, cypher_version: str | None = None,
                      store_base: str | None = None):
        # `store_base` (the store's `uri("")`): the seed URI must be inside it (#77)
        if store_base is not None and not seed_uri.startswith(store_base):
            raise ValueError(f"seed URI {seed_uri!r} is not in the backup store {store_base!r}")
        prefix = f"CYPHER {cypher_version} " if cypher_version else ""
        topo = ""
        if topology is not None:
            topo = (f" TOPOLOGY {topology.primaries} PRIMARIES "
                    f"{topology.secondaries} SECONDARIES")
        opts = f"seedURI: '{seed_uri_literal(seed_uri)}'"
        if cypher_version == "5":
            opts += ", existingData: 'use'"
        if restore_until:
            from .until import parse_until  # normalised here too: never the raw input (#77)

            opts += f", seedRestoreUntil: datetime('{parse_until(restore_until)}')"
        self.run_system(f"{prefix}CREATE DATABASE {quote(name)}{topo} OPTIONS {{ {opts} }} WAIT")

    def alter_alias(self, alias: str, target: str):
        self.run_system(f"ALTER ALIAS {quote(alias)} SET DATABASE TARGET {quote(target)}")

    def create_alias(self, alias: str, target: str):
        self.run_system(f"CREATE ALIAS {quote(alias)} IF NOT EXISTS FOR DATABASE {quote(target)}")

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

    def database_created(self, name: str):
        """When the database's current store was created (UTC datetime), or None if it doesn't
        exist. A by-name DROP + CREATE gives a new creation time under the same name (#74)."""
        rows = self.run_system(
            "SHOW DATABASE $n YIELD name, creationTime RETURN creationTime", n=name)
        times = [r["creationTime"] for r in rows if r.get("creationTime") is not None]
        if not times:
            return None
        t = min(times)
        return t.to_native() if hasattr(t, "to_native") else t

    def database_access(self, name: str) -> str | None:
        """`read-write` or `read-only` (SHOW DATABASES `access`), or None if absent (#112)."""
        rows = self.run_system("SHOW DATABASE $n YIELD name, access RETURN access", n=name)
        return rows[0]["access"] if rows else None

    def set_read_only(self, name: str, read_only: bool = True):
        """ALTER DATABASE … SET ACCESS READ ONLY / READ WRITE (#112)."""
        mode = "READ ONLY" if read_only else "READ WRITE"
        self.run_system(f"ALTER DATABASE {quote(name)} SET ACCESS {mode}")

    def stop_database(self, name: str):
        self.run_system(f"STOP DATABASE {quote(name)} WAIT")

    def drop_database(self, name: str):
        self.run_system(f"DROP DATABASE {quote(name)} IF EXISTS WAIT")

    def count_nodes(self, database: str) -> int:
        return self.run_on(database, "MATCH (n) RETURN count(n) AS n")[0]["n"]

    def list_databases(self) -> list[str]:
        return [r["name"] for r in self.run_system("SHOW DATABASES YIELD name")]

    def privilege_commands(self, database: str) -> list[str]:
        """Every GRANT/DENY whose ON clause names `database` (#117)."""
        from .privileges import scoped_to

        rows = self.run_system("SHOW PRIVILEGES AS COMMANDS YIELD command RETURN command")
        return scoped_to([r["command"] for r in rows], database)

    def list_aliases(self) -> dict[str, str]:
        rows = self.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name, database RETURN name, database"
        )
        return {r["name"]: r["database"] for r in rows}


LOAD_DIR = "/_seed/"  # load artifacts (#156) live in a `_seed/` folder under each member


class DeleteError(RuntimeError):
    """Some keys could not be deleted. `deleted` is how many were; `failed` lists
    (key, code, message) for the rest."""

    def __init__(self, deleted: int, failed: list[tuple]):
        self.deleted, self.failed = deleted, failed
        shown = ", ".join(f"{k} ({c})" for k, c, _m in failed[:10])
        more = f" and {len(failed) - 10} more" if len(failed) > 10 else ""
        super().__init__(f"deleted {deleted}; failed to delete {len(failed)}: {shown}{more}")


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
    def list_load_artifacts(self, prefix: str): ...
    def create_exclusive(self, key: str, text: str) -> bool: ...
    def list_prefixes(self, prefix: str = "") -> list[str]: ...


def object_store(bucket: str, endpoint_url: str | None = None, region: str = "us-east-1",
                 sse: str | None = None, sse_kms_key_id: str | None = None,
                 write_args_json: str = "{}", cloud: str | None = None) -> ObjectStore:
    """Build the object-store backend for `cloud` (aws (default) | azure | gcp)."""
    c = (cloud or "aws").lower()
    if c in ("azure", "az", "azb"):
        return AzureObjectStore(bucket, endpoint_url, region, sse, sse_kms_key_id, write_args_json)
    if c in ("gcp", "gcs", "google"):
        return GcsObjectStore(bucket, endpoint_url, region, sse, sse_kms_key_id, write_args_json)
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

    def list_load_artifacts(self, prefix: str) -> list[tuple[str, object, str]]:
        """Load artifacts under `prefix` (#98, #156): `.dump` and `.backup` files in a `_seed/`
        folder, as (key, modified, kind) oldest first. Kept apart from `list_artifacts`, so a
        load never becomes an alias's chain head."""
        out = []
        for suffix, kind in ((".dump", "dump"), (".backup", "backup")):
            out += [(k, m, kind) for (k, m) in self.list_text_keys(prefix, suffix)
                    if LOAD_DIR in k]
        return sorted(out, key=lambda t: (t[1], t[0]))

    def admin_env(self) -> dict[str, str]:
        """Environment neo4j-admin needs to reach this store (#105): a named store's region,
        runner endpoint and credentials. Empty for the environment's own store."""
        return dict(getattr(self, "admin_env_vars", {}) or {})

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
        # a named store's own key pair (#105); None: the process's default AWS credentials
        self.credentials: dict | None = None
        # environment for neo4j-admin when it reads/writes this store (#105); {} = inherit
        self.admin_env_vars: dict[str, str] = {}

    def _client(self):
        import boto3
        from botocore.config import Config

        cfg = Config(s3={"addressing_style": "path"}) if self.endpoint_url else None
        creds = {}
        if self.credentials:
            creds = {"aws_access_key_id": self.credentials["access_key_id"],
                     "aws_secret_access_key": self.credentials["secret_access_key"],
                     "aws_session_token": self.credentials.get("session_token")}
        return boto3.client(
            "s3", endpoint_url=self.endpoint_url, region_name=self.region, config=cfg, **creds
        )

    def list_prefixes(self, prefix: str = "") -> list[str]:
        """The 'folders' directly under `prefix` (#124), each ending in '/'."""
        out = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            out += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
        return out

    def list_artifacts(self, prefix: str):
        out = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".backup") and LOAD_DIR not in o["Key"]:
                    out.append((o["Key"], o["Size"], o["LastModified"]))
        return out

    def object_size(self, key: str) -> int:
        return self._client().head_object(Bucket=self.bucket, Key=key)["ContentLength"]

    _DELETE_BATCH = 1000  # S3 DeleteObjects takes at most 1000 keys per request

    def delete_keys(self, keys: list[str]) -> int:
        """Delete `keys` in batches of 1000 (#76). Every batch is attempted; per-key errors from
        the response are collected and raised together afterwards (`DeleteError`, which carries
        the number deleted and the failures). Returns the number actually deleted."""
        keys = list(keys)
        if not keys:
            return 0
        client = self._client()
        deleted, failed = 0, []
        for i in range(0, len(keys), self._DELETE_BATCH):
            batch = keys[i:i + self._DELETE_BATCH]
            resp = client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = resp.get("Errors", []) or []
            failed += [(e.get("Key"), e.get("Code"), e.get("Message")) for e in errors]
            deleted += len(batch) - len(errors)
        if failed:
            raise DeleteError(deleted, failed)
        return deleted

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

    def create_exclusive(self, key: str, text: str) -> bool:
        """Write `key` only if it doesn't exist (S3 conditional write, `If-None-Match: *`);
        False when it already exists. Atomic; used for the per-group lock (#108)."""
        from botocore.exceptions import ClientError

        try:
            self._client().put_object(Bucket=self.bucket, Key=key, Body=text.encode(),
                                      IfNoneMatch="*", **self.write_args)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("PreconditionFailed",
                                                            "ConditionalRequestConflict"):
                return False
            raise

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

    def list_prefixes(self, prefix: str = "") -> list[str]:
        return [b.name for b in self._c().walk_blobs(name_starts_with=prefix, delimiter="/")
                if b.name.endswith("/")]

    def list_artifacts(self, prefix: str):
        return [(b.name, b.size, b.last_modified)
                for b in self._c().list_blobs(name_starts_with=prefix)
                if b.name.endswith(".backup") and LOAD_DIR not in b.name]

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

    def create_exclusive(self, key: str, text: str) -> bool:
        from azure.core.exceptions import ResourceExistsError

        try:
            self._c().upload_blob(name=key, data=text.encode(), overwrite=False)
            return True
        except ResourceExistsError:
            return False

    def list_text_keys(self, prefix: str, suffix: str = ".cypher"):
        return [(b.name, b.last_modified)
                for b in self._c().list_blobs(name_starts_with=prefix) if b.name.endswith(suffix)]


class GcsObjectStore(_BaseObjectStore):
    """Google Cloud Storage backend (#52). `bucket` is the GCS bucket. Real GCS uses ADC
    (GOOGLE_APPLICATION_CREDENTIALS / workload identity); the **fake-gcs-server** emulator is
    selected by `STORAGE_EMULATOR_HOST`. Neo4j's own `gs://` fetch has no emulator endpoint
    override, so *restore* is validated via `file://` (phase 4), not against fake-gcs."""

    def __init__(self, bucket: str, endpoint_url: str | None = None, region: str = "us-east-1",
                 sse: str | None = None, sse_kms_key_id: str | None = None,
                 write_args_json: str = "{}"):
        self.bucket_name = bucket
        self.write_args: dict = json.loads(write_args_json or "{}")

    def _b(self):
        from google.cloud import storage

        endpoint = os.environ.get("STORAGE_EMULATOR_HOST")
        if endpoint:
            from google.auth.credentials import AnonymousCredentials

            client = storage.Client(project=os.environ.get("GCP_PROJECT", "test"),
                                    credentials=AnonymousCredentials(),
                                    client_options={"api_endpoint": endpoint})
        else:
            client = storage.Client()
        return client.bucket(self.bucket_name)

    def list_prefixes(self, prefix: str = "") -> list[str]:
        it = self._b().list_blobs(prefix=prefix, delimiter="/")
        for _page in it.pages:  # prefixes are filled in as the pages are read
            pass
        return sorted(it.prefixes)

    def list_artifacts(self, prefix: str):
        return [(b.name, b.size, b.updated)
                for b in self._b().list_blobs(prefix=prefix)
                if b.name.endswith(".backup") and LOAD_DIR not in b.name]

    def object_size(self, key: str) -> int:
        return self._b().get_blob(key).size

    def delete_keys(self, keys: list[str]) -> int:
        bkt = self._b()
        for k in keys:
            bkt.blob(k).delete()
        return len(keys)

    def _copy(self, src_key: str, dst_key: str) -> None:
        bkt = self._b()
        bkt.copy_blob(bkt.blob(src_key), bkt, dst_key)

    def _download(self, key: str, local_path: str) -> None:
        self._b().blob(key).download_to_filename(local_path)

    def uri(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{key}"

    def upload_file(self, local_path: str, key: str) -> str:
        self._b().blob(key).upload_from_filename(local_path)
        return key

    def put_text(self, key: str, text: str) -> str:
        self._b().blob(key).upload_from_string(text, content_type="text/plain; charset=utf-8")
        return key

    def get_text(self, key: str) -> str:
        return self._b().blob(key).download_as_bytes().decode()

    def create_exclusive(self, key: str, text: str) -> bool:
        from google.api_core.exceptions import PreconditionFailed

        try:
            self._b().blob(key).upload_from_string(text, if_generation_match=0)
            return True
        except PreconditionFailed:
            return False

    def list_text_keys(self, prefix: str, suffix: str = ".cypher"):
        return [(b.name, b.updated)
                for b in self._b().list_blobs(prefix=prefix) if b.name.endswith(suffix)]


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

    def inspect_command(self, location: str) -> list:
        """`neo4j-admin backup inspect` over a backup folder or one artifact (#118): JSON records
        with type, transaction range, time and store id. `--empty` keeps diffs with no new
        transactions; `--verbose` adds the `neo4j <version>` line."""
        return [
            *self.exec_prefix, self.neo4j_admin, "backup", "inspect",
            "--format=JSON", "--empty", "--verbose", location,
        ]

    def dump_command(self, database: str, to_path: str) -> list:
        """`neo4j-admin database dump` of an offline store (#158): one `<database>.dump` in
        `to_path` (a local folder or a cloud URI), overwriting a file of that name."""
        return [
            *self.exec_prefix, self.neo4j_admin, "database", "dump",
            f"--to-path={to_path}", "--overwrite-destination=true", database,
        ]

    def import_dump_command(self, database: str, source_args: list, to_path: str,
                            data_root: str = "/data") -> list:
        """One shell step (#167): import into an offline store, dump it to `to_path`, then remove
        the store, keeping the exit code. One step, so it works pod-per-command, and the loader
        is left clean for the next load under the same name."""
        import shlex

        imp = [self.neo4j_admin, "database", "import", "full", database, *source_args]
        dump = [self.neo4j_admin, "database", "dump", f"--to-path={to_path}",
                "--overwrite-destination=true", database]
        clean = ["rm", "-rf", *(f"{data_root}/{d}/{database}"
                                for d in ("databases", "transactions"))]
        script = f"{shlex.join(imp)} && {shlex.join(dump)}; rc=$?; {shlex.join(clean)}; exit $rc"
        return [*self.exec_prefix, "sh", "-c", script]

    def command_build_command(self, build_cmd: list, database: str, work: str, to_path: str,
                              import_args: list | None, output: str, timeout: int,
                              env_names=(), data_root: str = "/data") -> list:
        """One runner step for a `kind: command` build (#109): a fresh work dir, the customer's
        command under `timeout`, then the built-in import of what it wrote (`output: import`) or
        its `<database>.dump` (`output: dump`), the dump to `to_path`, and cleanup. Exit code
        kept; a missing output fails the step."""
        import shlex

        q = shlex.join
        steps = [f"rm -rf {shlex.quote(work)}", f"mkdir -p {shlex.quote(work)}",
                 f"cd {shlex.quote(work)}", f"timeout {int(timeout)} {q(build_cmd)}"]
        if output == "dump":
            dumpfile = f"{work}/{database}.dump"
            steps += [f"test -s {shlex.quote(dumpfile)} || {{ echo 'build left no "
                      f"{database}.dump in the work dir' >&2; false; }}",
                      q([self.neo4j_admin, "database", "load",
                         f"--from-path={work}", "--overwrite-destination=true", database])]
        else:
            steps.append(q([self.neo4j_admin, "database", "import", "full", database,
                            *(import_args or [])]))
        steps.append(q([self.neo4j_admin, "database", "dump", f"--to-path={to_path}",
                        "--overwrite-destination=true", database]))
        clean = q(["rm", "-rf", work, *(f"{data_root}/{d}/{database}"
                                        for d in ("databases", "transactions"))])
        script = " && ".join(steps) + f"; rc=$?; {clean}; exit $rc"
        return [*self.exec_prefix, "sh", "-c", script]

    def export_command(self, from_path: str, database: str, to_path: str,
                       until: str | None = None, data_root: str = "/data") -> list:
        """One shell step (#160), so it also works when each command is its own pod: restore
        the chain at `from_path` (optionally `--restore-until`) into a throwaway local store
        `database` on the runner, dump it to `to_path`, then remove the local store. The cluster
        is never touched."""
        import shlex

        restore = [self.neo4j_admin, "database", "restore", f"--from-path={from_path}",
                   f"--temp-path={self.scratch_path}", "--overwrite-destination=true"]
        if until:
            restore.append(f"--restore-until={until}")
        restore.append(database)
        dump = [self.neo4j_admin, "database", "dump", f"--to-path={to_path}",
                "--overwrite-destination=true", database]
        clean = ["rm", "-rf", *(f"{data_root}/{d}/{database}"
                                for d in ("databases", "transactions", "scripts"))]
        script = (f"{shlex.join(restore)} && {shlex.join(dump)}; rc=$?; {shlex.join(clean)}; "
                  f"exit $rc")
        return [*self.exec_prefix, "sh", "-c", script]

    def import_command(self, database: str, source_args: list) -> list:
        """Off-cluster bulk import (#16): `neo4j-admin database import full <database> <source_args…>`.
        `source_args` (the `--nodes`/`--relationships`/options) is a **passthrough** — dataset-specific
        and sensitive, so we structure the call but do not model the args. The database comes **first**:
        `--nodes`/`--relationships` are multi-value, so a trailing database would be swallowed as an
        extra file. Runs on the loader (writes the offline store into the DBMS data dir)."""
        return [
            *self.exec_prefix, self.neo4j_admin, "database", "import", "full",
            database, *source_args,
        ]

    def env(self) -> dict:
        return {"HEAP_SIZE": self.heap_size}

    def with_env(self, cmd: list, names) -> list:
        """`cmd` with `-e NAME` for each name after the `exec` of a `docker … exec` prefix, so the
        container inherits those variables from the caller's environment (values never appear in
        the argv). Other prefixes, or none, pass the environment through the process (#105)."""
        names = list(names)
        n = len(self.exec_prefix)
        if not names or not n or "exec" not in self.exec_prefix:
            return cmd
        i = self.exec_prefix.index("exec") + 1
        flags = [x for name in names for x in ("-e", name)]
        return [*cmd[:i], *flags, *cmd[i:]]

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
