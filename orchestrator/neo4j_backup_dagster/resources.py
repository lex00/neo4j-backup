"""Dagster resources: Bolt (restore Cypher), object store (artifact lookup), and the
backup runner (neo4j-admin tuning). Mirrors the validated shell behavior.
"""

from __future__ import annotations

from contextlib import contextmanager

import dagster as dg


class Neo4jResource(dg.ConfigurableResource):
    """Bolt client for system-database operations: seed-from-URI and alias swap.

    This is the agentless restore surface — pure Cypher over the wire, nothing on the
    instance (DESIGN.md §3).
    """

    uri: str = "neo4j://localhost:7687"
    user: str = "neo4j"
    password: str

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

    # --- restore primitives (CloudSeedProvider takes region/endpoint from server env;
    #     no seedConfig — validated) ---
    def seed_database(self, name: str, seed_uri: str, restore_until: str | None = None):
        # `existingData` is deprecated (removed without replacement) — omit it.
        opts = f"seedURI: '{seed_uri}'"
        if restore_until:
            opts += f", seedRestoreUntil: datetime('{restore_until}')"
        self.run_system(f"CREATE DATABASE `{name}` OPTIONS {{ {opts} }} WAIT")

    def alter_alias(self, alias: str, target: str):
        self.run_system(f"ALTER ALIAS `{alias}` SET DATABASE TARGET `{target}`")

    def create_alias(self, alias: str, target: str):
        self.run_system(
            f"CREATE ALIAS `{alias}` IF NOT EXISTS FOR DATABASE `{target}`"
        )

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


class ObjectStoreResource(dg.ConfigurableResource):
    """S3-compatible object store. Per-group bucket with SSE-KMS default encryption."""

    bucket: str
    endpoint_url: str | None = None  # MinIO locally; None for AWS
    region: str = "us-east-1"

    def _client(self):
        import boto3
        from botocore.config import Config

        # MinIO / S3-compatible endpoints want path-style addressing.
        cfg = Config(s3={"addressing_style": "path"}) if self.endpoint_url else None
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            config=cfg,
        )

    def list_artifacts(self, prefix: str):
        """[(key, size_bytes, last_modified)] for *.backup under a prefix (paginated)."""
        out = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".backup"):
                    out.append((o["Key"], o["Size"], o["LastModified"]))
        return out

    def latest_artifact_key(self, prefix: str) -> str | None:
        """Newest *.backup key under a prefix, by last-modified (the chain head)."""
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
        """Server-side copy every *.backup under src_prefix to dst_prefix (for
        non-destructive verification). Returns the count copied."""
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


class RunnerResource(dg.ConfigurableResource):
    """neo4j-admin backup tuning (the validated memory + scratch caps, DESIGN.md §5.5).

    `backup_command` builds the argv; execution is via Pipes (subprocess locally,
    PipesK8sClient in production — see definitions.py).
    """

    backup_source: str = "neo4j:6362"  # prefer a follower/secondary in a cluster
    scratch_path: str = "/scratch"  # sized to the largest full (multi-TB)
    pagecache: str = "512M"  # MUST be explicit or it inherits server pagecache -> OOM
    heap_size: str = "2G"  # HEAP_SIZE env on the process
    neo4j_admin: str = "neo4j-admin"
    # Wrap the command, e.g. ["docker","compose","exec","-T","runner"] locally so it
    # runs in the runner container. Empty in prod (Dagster runs in the runner pod).
    exec_prefix: list[str] = []

    def backup_command(
        self, database: str, to_path: str, kind: str = "AUTO"
    ) -> list[str]:
        return [
            *self.exec_prefix,
            self.neo4j_admin, "database", "backup",
            "--from", self.backup_source,
            "--to-path", to_path,
            "--temp-path", self.scratch_path,
            "--pagecache", self.pagecache,
            "--type", kind,           # AUTO | FULL | DIFF (full/diff lanes)
            "--compress=true",
            database,
        ]

    def aggregate_command(
        self, database: str, from_path: str, keep_old: bool = False
    ) -> list[str]:
        """Collapse a backup chain into a single recovered full (RTO + makes it
        consistency-checkable). `neo4j-admin backup aggregate` (the
        `database aggregate-backup` name is deprecated). Writes in place at from_path.
        """
        cmd = [
            *self.exec_prefix, self.neo4j_admin, "backup", "aggregate",
            f"--from-path={from_path}", f"--temp-path={self.scratch_path}",
        ]
        if keep_old:
            cmd.append("--keep-old-backup=true")
        cmd.append(database)
        return cmd

    def check_command(
        self, database: str, from_path: str, max_off_heap: str = "50%"
    ) -> list[str]:
        """Consistency-check a RECOVERED FULL artifact directly (from s3:// too). Exit 0
        = consistent; non-zero + an inconsistencies-*.report on failure. Not supported
        on differential / unrecovered-full artifacts — aggregate first.
        """
        return [
            *self.exec_prefix, self.neo4j_admin, "database", "check",
            f"--from-path={from_path}", f"--temp-path={self.scratch_path}",
            f"--max-off-heap-memory={max_off_heap}", database,
        ]

    def env(self) -> dict[str, str]:
        return {"HEAP_SIZE": self.heap_size}

    # --- k8s mode (PipesK8sClient): each backup runs in its own pod -------------
    mode: str = "subprocess"          # "subprocess" (VM/EC2, validated) | "k8s"
    image: str = ""                   # Neo4j image with neo4j-admin (k8s mode)
    node_selector_json: str = "{}"    # e.g. '{"workload":"neo4j-backup"}'
    memory_limit: str = "4Gi"
    scratch_storage: str = "6Ti"      # per-backup ephemeral PVC size
    service_account: str = ""         # IRSA / workload identity for S3 + KMS
    extra_env_json: str = "{}"        # extra pod env (e.g. AWS creds/endpoint w/o IRSA)

    def k8s_pod_spec(self, env: dict) -> dict:
        """base_pod_spec for PipesK8sClient.run — a container with the memory limit,
        HEAP_SIZE env, and a fresh ephemeral scratch PVC mounted at --temp-path.
        extra_env_json supplies S3 creds/endpoint where IRSA is not used (e.g. MinIO)."""
        import json

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
