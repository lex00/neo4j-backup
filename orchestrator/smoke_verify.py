"""Drive aggregate + verify (Phase 5) through Dagster against the local stack.

Aggregates the pitr-demo chain into a recovered full, then consistency-checks it from
S3 — both via Pipes/neo4j-admin in the runner container.

    orchestrator/.venv/bin/python orchestrator/smoke_verify.py
"""

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

import dagster as dg

from neo4j_backup_dagster.definitions import aggregate_chain, verify
from neo4j_backup_dagster.resources import ObjectStoreResource, RunnerResource
from dagster_k8s import PipesK8sClient

EXEC_PREFIX = [
    "docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml",
    "exec", "-T", "runner",
]
RESOURCES = {
    "store": ObjectStoreResource(
        bucket="neo4j-backups", endpoint_url="http://localhost:9000", region="us-east-1"
    ),
    "runner": RunnerResource(
        scratch_path="/scratch", pagecache="512M", exec_prefix=EXEC_PREFIX
    ),
    "pipes_subprocess_client": dg.PipesSubprocessClient(),
    "pipes_k8s_client": PipesK8sClient(),
}
CFG = {"database": "pitr-demo", "prefix": "pitr/pitr-demo"}


def main() -> None:
    print("== AGGREGATE chain via Dagster ==")
    a = aggregate_chain.execute_in_process(
        run_config={"ops": {"aggregate_op": {"config": CFG}}}, resources=RESOURCES
    )
    assert a.success
    print("   aggregate OK ->", a.output_for_node("aggregate_op"))

    print("== VERIFY (consistency check) via Dagster ==")
    v = verify.execute_in_process(
        run_config={"ops": {"verify_op": {"config": CFG}}}, resources=RESOURCES
    )
    assert v.success, "verify failed"
    print("   verify OK ->", v.output_for_node("verify_op"))
    print("PASS: aggregate + consistency-check driven through Dagster")


if __name__ == "__main__":
    main()
