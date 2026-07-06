"""Dagster resources — thin `ConfigurableResource` wrappers that delegate to the
orchestrator-agnostic clients in `neo4j_backup_core.clients`. The logic lives in core;
these just carry config and forward.
"""

from __future__ import annotations

import dagster as dg

from neo4j_backup_core.clients import BackupRunner, Neo4jClient, ObjectStore


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
        if self.password is not None:
            return Neo4jClient(self.uri, self.user, self.password)
        from neo4j_backup_core import secrets

        provider = secrets.build(self.secret_provider)
        ref = self.password_ref
        return Neo4jClient(self.uri, self.user, lambda: provider.resolve(ref))

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


class ObjectStoreResource(dg.ConfigurableResource):
    bucket: str
    endpoint_url: str | None = None
    region: str = "us-east-1"

    def _core(self) -> ObjectStore:
        return ObjectStore(self.bucket, self.endpoint_url, self.region)

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

    def s3_uri(self, *a, **k):
        return self._core().s3_uri(*a, **k)

    def put_text(self, *a, **k):
        return self._core().put_text(*a, **k)

    def get_text(self, *a, **k):
        return self._core().get_text(*a, **k)

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

    def _core(self) -> BackupRunner:
        return BackupRunner(
            self.backup_source, self.scratch_path, self.pagecache, self.heap_size,
            self.neo4j_admin, self.exec_prefix, self.mode, self.image,
            self.node_selector_json, self.memory_limit, self.scratch_storage,
            self.service_account, self.extra_env_json,
        )

    def backup_command(self, *a, **k):
        return self._core().backup_command(*a, **k)

    def aggregate_command(self, *a, **k):
        return self._core().aggregate_command(*a, **k)

    def check_command(self, *a, **k):
        return self._core().check_command(*a, **k)

    def env(self, *a, **k):
        return self._core().env(*a, **k)

    def k8s_pod_spec(self, *a, **k):
        return self._core().k8s_pod_spec(*a, **k)
