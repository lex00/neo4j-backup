"""Build the core clients from the runtime environment — shared by the Airflow adapter and the CLI
(the Dagster adapter carries the same wiring in its `ConfigurableResource`s instead). Imports no
orchestrator; the same env table documented in orchestrator/README applies.
"""

from __future__ import annotations

import json
import os
import subprocess

from . import secrets
from .clients import BackupRunner, Neo4jClient, ObjectStore, object_store


def subprocess_admin(runner, stdout=None):
    """A `run_admin(cmd)` callable for `ops.*` that runs one neo4j-admin command as a local
    subprocess with the runner's environment (raises on non-zero). Shared by the CLI and the MCP
    server; both pass `stdout=sys.stderr` so the child's output never lands on their own stdout
    (the CLI's JSON envelope / the MCP stdio protocol)."""
    def run(cmd):
        subprocess.run(cmd, check=True, env={**os.environ, **runner.env()}, stdout=stdout)
    return run


def policy_path() -> str:
    return os.environ.get("NEO4J_BACKUP_POLICY", "policies/demo.yaml")


def neo4j() -> Neo4jClient:
    # Credential via a secret provider (#18), resolved lazily per connect. Default
    # SECRET_PROVIDER=env reads NEO4J_PASSWORD; aws-sm uses NEO4J_PASSWORD_REF (secret id/ARN).
    provider = secrets.from_env()
    ref = os.environ.get("NEO4J_PASSWORD_REF")
    return Neo4jClient(
        os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"),
        os.environ.get("NEO4J_USER", "neo4j"),
        lambda: provider.resolve(ref),
    )


def store() -> ObjectStore:
    return object_store(
        os.environ.get("BACKUP_BUCKET", "neo4j-backups"),
        os.environ.get("AWS_ENDPOINT_URL_S3") or None,  # unset on real AWS
        os.environ.get("AWS_REGION", "us-east-1"),
        sse=os.environ.get("S3_SSE") or None,
        sse_kms_key_id=os.environ.get("S3_SSE_KMS_KEY_ID") or None,
        write_args_json=os.environ.get("S3_WRITE_ARGS", "{}"),
        cloud=os.environ.get("CLOUD") or None,  # aws (default) | azure | gcp
    )


def runner(exec_prefix=None) -> BackupRunner:
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
