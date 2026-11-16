"""Drive verify and aggregate through Dagster against the local stack.

For the partition `demo/acme-orders`: take a FULL and a DIFF (the CLI), materialize `verify`
(copies the chain aside, aggregates the copy, `database check`), then `aggregate` (collapses
the live chain into one recovered full, in place), then `verify` again on the aggregated chain.
All neo4j-admin work runs via Pipes in the runner container.

Prereqs: `just fresh`.   orchestrator/.venv/bin/python orchestrator/smoke_verify.py
"""

import json
import os
import subprocess
import sys

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

import dagster as dg  # noqa: E402
from neo4j import GraphDatabase  # noqa: E402

from neo4j_backup_dagster.definitions import aggregate, targets, verify  # noqa: E402
from neo4j_backup_dagster.resources import ObjectStoreResource, RunnerResource  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXEC_PREFIX = [
    "docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml",
    "exec", "-T", "runner",
]
PARTITION = "demo/acme-orders"
RESOURCES = {
    "store": ObjectStoreResource(
        bucket="neo4j-backups", endpoint_url="http://localhost:9000", region="us-east-1"
    ),
    "runner": RunnerResource(
        backup_source="neo4j:6362", scratch_path="/scratch", pagecache="512M",
        heap_size="2G", exec_prefix=EXEC_PREFIX,
    ),
    "pipes_subprocess_client": dg.PipesSubprocessClient(),
}


def cli(*args) -> None:
    env = {**os.environ, "NEO4J_BOLT_URI": "neo4j://localhost:7687",
           "NEO4J_PASSWORD": "devpassword", "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
           "SCRATCH_PATH": "/scratch", "RUNNER_EXEC_PREFIX": json.dumps(EXEC_PREFIX),
           "BACKUP_UPLOAD": "admin", "BACKUP_BUCKET": "neo4j-backups", "AWS_REGION": "us-east-1",
           "NEO4J_BACKUP_POLICY": "policies/demo.yaml"}
    p = subprocess.run([os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"),
                        "--json", *args], cwd=REPO, env=env, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr[-1500:]


def materialize(asset, instance):
    res = dg.materialize([asset], partition_key=PARTITION, resources=RESOURCES,
                         instance=instance)
    assert res.success, f"{asset.key.to_user_string()} failed"
    return res


def main() -> None:
    os.chdir(REPO)
    cli("backup", "demo", "--kind", "FULL")
    with GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "devpassword")) as d, \
            d.session(database="acme-orders") as s:
        s.run("CREATE (:SmokeVerify {n: 1})").consume()
    cli("backup", "demo", "--kind", "DIFF")
    print("== FULL + DIFF of demo taken ==")
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(targets.name, [PARTITION])
        materialize(verify, instance)
        print("== verify (copy aside, aggregate the copy, database check): OK ==")
        materialize(aggregate, instance)
        print("== aggregate (live chain collapsed into one recovered full): OK ==")
        materialize(verify, instance)
        print("== verify on the aggregated chain: OK ==")
    print("PASS: verify + aggregate driven through Dagster")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
