"""Drive the backup asset and the restore job THROUGH Dagster, in-process, against the
local stack (STACK.md). Proves the code location works end-to-end.

Run from the repo root with the orchestrator venv:
    orchestrator/.venv/bin/python orchestrator/smoke_local.py
"""

import os

# boto3 (ObjectStoreResource) reads MinIO creds from the environment.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

import dagster as dg

from neo4j_backup_dagster.definitions import backup, restore_group, targets
from neo4j_backup_dagster.resources import (
    Neo4jResource,
    ObjectStoreResource,
    RunnerResource,
)

# Local: run neo4j-admin inside the runner container. Prod leaves exec_prefix empty.
EXEC_PREFIX = [
    "docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml",
    "exec", "-T", "runner",
]
PARTITION = "demo/acme-orders"

RESOURCES = {
    "neo4j": Neo4jResource(
        uri="neo4j://localhost:7687", user="neo4j", password="devpassword"
    ),
    "store": ObjectStoreResource(
        bucket="neo4j-backups", endpoint_url="http://localhost:9000", region="us-east-1"
    ),
    "runner": RunnerResource(
        backup_source="neo4j:6362", scratch_path="/scratch",
        pagecache="512M", heap_size="2G", exec_prefix=EXEC_PREFIX,
    ),
    "pipes_subprocess_client": dg.PipesSubprocessClient(),
}


def main() -> None:
    neo4j: Neo4jResource = RESOURCES["neo4j"]

    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(targets.name, [PARTITION])

        print(f"== BACKUP via Dagster (Pipes): partition {PARTITION} ==")
        res = dg.materialize(
            [backup],
            partition_key=PARTITION,
            resources=RESOURCES,
            run_config={"ops": {"backup": {"config": {"kind": "AUTO"}}}},
            instance=instance,
        )
        assert res.success, "backup materialization failed"
        print("   backup OK")

        print("== RESTORE group via Dagster job (Bolt Cypher) ==")
        rr = restore_group.execute_in_process(
            run_config={"ops": {"restore_group_op": {"config": {"group_id": "demo"}}}},
            resources={"neo4j": RESOURCES["neo4j"], "store": RESOURCES["store"]},
            instance=instance,
        )
        assert rr.success, "restore job failed"
        print("   restore OK")

    # Verify through the alias (the app's view).
    rows = neo4j.run_system(
        "SHOW ALIASES FOR DATABASE YIELD name, database "
        "WHERE name = 'acme-orders' RETURN database"
    )
    target = rows[0]["database"] if rows else None
    with neo4j._driver() as d, d.session(database="acme-orders") as s:
        n = s.run("MATCH (c:Customer) RETURN count(c) AS n").single()["n"]
    print(f"== VERIFY: alias acme-orders -> {target}, {n} customers via alias ==")
    assert n >= 2, f"expected restored data, got {n}"
    print("PASS: backup + restore driven through Dagster against the live stack")


if __name__ == "__main__":
    main()
