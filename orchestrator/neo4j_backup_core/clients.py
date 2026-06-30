"""Plain, framework-free clients: Bolt (restore Cypher), object store (boto3), and the
neo4j-admin runner (command building + k8s pod spec). The Dagster/Airflow adapters wrap
these; the logic lives here once.
"""

from __future__ import annotations

import json
from contextlib import contextmanager


class Neo4jClient:
    """Bolt client for system-database operations: seed-from-URI and alias swap. The
    agentless restore surface — pure Cypher, nothing on the instance (DESIGN.md §3)."""

    def __init__(self, uri: str = "neo4j://localhost:7687", user: str = "neo4j",
                 password: str = ""):
        self.uri, self.user, self.password = uri, user, password

    @contextmanager
    def _driver(self):
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        try:
            yield driver
        finally:
            driver.close()

    def run_system(self, cypher: str, **params):
        with self._driver() as d, d.session(database="system") as s:
            return [r.data() for r in s.run(cypher, **params)]

    # CloudSeedProvider takes region/endpoint from server env; no seedConfig, and
    # `existingData` is deprecated — both omitted (validated, see RECOVERY.md).
    def seed_database(self, name: str, seed_uri: str, restore_until: str | None = None):
        opts = f"seedURI: '{seed_uri}'"
        if restore_until:
            opts += f", seedRestoreUntil: datetime('{restore_until}')"
        self.run_system(f"CREATE DATABASE `{name}` OPTIONS {{ {opts} }} WAIT")

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

    def stop_database(self, name: str):
        self.run_system(f"STOP DATABASE `{name}` WAIT")

    def drop_database(self, name: str):
        self.run_system(f"DROP DATABASE `{name}` IF EXISTS WAIT")

    def count_nodes(self, database: str) -> int:
        with self._driver() as d, d.session(database=database) as s:
            return s.run("MATCH (n) RETURN count(n) AS n").single()["n"]

    def list_databases(self) -> list[str]:
        return [r["name"] for r in self.run_system("SHOW DATABASES YIELD name")]

    def list_aliases(self) -> dict[str, str]:
        rows = self.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name, database RETURN name, database"
        )
        return {r["name"]: r["database"] for r in rows}


class ObjectStore:
    """S3-compatible object store (boto3). Per-group bucket with SSE-KMS default encryption."""

    def __init__(self, bucket: str, endpoint_url: str | None = None,
                 region: str = "us-east-1"):
        self.bucket, self.endpoint_url, self.region = bucket, endpoint_url, region

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

    def latest_artifact_key(self, prefix: str) -> str | None:
        arts = self.list_artifacts(prefix)
        return max(arts, key=lambda t: t[2])[0] if arts else None

    def object_size(self, key: str) -> int:
        return self._client().head_object(Bucket=self.bucket, Key=key)["ContentLength"]

    def delete_keys(self, keys: list[str]) -> int:
        if not keys:
            return 0
        self._client().delete_objects(
            Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )
        return len(keys)

    def copy_prefix(self, src_prefix: str, dst_prefix: str) -> int:
        c = self._client()
        n = 0
        for key, _s, _m in self.list_artifacts(src_prefix):
            dst = dst_prefix + key[len(src_prefix):]
            c.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": key},
                Key=dst,
            )
            n += 1
        return n

    def delete_prefix(self, prefix: str) -> int:
        return self.delete_keys([k for (k, _s, _m) in self.list_artifacts(prefix)])

    def s3_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    # --- text artifacts (the logical metadata export; .cypher, not .backup) ---
    # No SSE header is set here: the bucket's default encryption (SSE-KMS) applies, exactly
    # as it does to the neo4j-admin .backup writes. The export carries no plaintext secrets
    # (passwords are not exported), but it is encrypted at rest like everything else.
    def put_text(self, key: str, text: str) -> str:
        self._client().put_object(
            Bucket=self.bucket, Key=key, Body=text.encode(),
            ContentType="text/plain; charset=utf-8",
        )
        return key

    def get_text(self, key: str) -> str:
        return self._client().get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()

    def latest_text_key(self, prefix: str, suffix: str = ".cypher") -> str | None:
        latest, ts = None, None
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(suffix) and (ts is None or o["LastModified"] > ts):
                    latest, ts = o["Key"], o["LastModified"]
        return latest


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
