"""Phase 6 smoke: exercise the per-store layout through Dagster against the live stack.

full backup -> mutate the live store -> differential backup (a REAL chain now, same
physical) -> non-destructive verify (copy+aggregate+check) -> group restore.

    orchestrator/.venv/bin/python orchestrator/smoke_phase6.py
"""

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

import dagster as dg

from neo4j_backup_dagster import definitions as D
from neo4j_backup_dagster.resources import Neo4jResource, ObjectStoreResource, RunnerResource

EXEC_PREFIX = [
    "docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml",
    "exec", "-T", "runner",
]
RES = {
    "neo4j": Neo4jResource(uri="neo4j://localhost:7687", user="neo4j", password="devpassword"),
    "store": ObjectStoreResource(bucket="neo4j-backups", endpoint_url="http://localhost:9000", region="us-east-1"),
    "runner": RunnerResource(backup_source="neo4j:6362", scratch_path="/scratch", pagecache="512M", exec_prefix=EXEC_PREFIX),
    "pipes_subprocess_client": dg.PipesSubprocessClient(),
}
ALIASES = ["acme-orders", "acme-graph", "acme-audit"]


def mat(asset, pk, instance, kind=None):
    rc = {"ops": {"backup": {"config": {"kind": kind}}}} if kind else None
    r = dg.materialize([asset], partition_key=pk, resources=RES, instance=instance, run_config=rc)
    assert r.success, f"{asset.key} {pk} failed"
    return r


def main() -> None:
    neo4j: Neo4jResource = RES["neo4j"]
    store: ObjectStoreResource = RES["store"]
    with dg.instance_for_test() as inst:
        for a in ALIASES:
            inst.add_dynamic_partitions(D.targets.name, [f"demo/{a}"])

        print("== FULL backup of each alias ==")
        for a in ALIASES:
            mat(D.backup, f"demo/{a}", inst, kind="FULL")

        print("== mutate live store + DIFFERENTIAL backup of acme-orders ==")
        phys = neo4j.alias_target("acme-orders")
        with neo4j._driver() as d, d.session(database=phys) as s:
            s.run("CREATE (:Customer {id:'C9', name:'Grace'})")
        mat(D.backup, "demo/acme-orders", inst, kind="DIFF")
        expected = neo4j.count_nodes(phys)  # state captured by the diff backup

        prefix = D._physical_prefix("demo", "acme-orders", phys)
        chain = store.list_artifacts(prefix)
        print(f"   chain under {prefix}: {len(chain)} artifacts (expect 2: full+diff)")
        assert len(chain) == 2, "expected a real 2-artifact chain in one physical prefix"

        print("== VERIFY acme-orders (non-destructive) ==")
        mat(D.verify, "demo/acme-orders", inst)
        after = store.list_artifacts(prefix)
        assert len(after) == 2, "verify must not mutate the prod chain"
        assert not store.list_artifacts("_verify/"), "verify scratch not cleaned up"
        print("   prod chain intact after verify; scratch cleaned")

        print("== RESTORE group ==")
        rr = D.restore_group.execute_in_process(
            run_config={"ops": {"restore_group_op": {"config": {"group_id": "demo"}}}},
            resources={"neo4j": RES["neo4j"], "store": RES["store"]},
            instance=inst,
        )
        assert rr.success

    n = neo4j.count_nodes("acme-orders")  # via alias -> the restored physical
    print(f"== acme-orders via alias after restore: {n} nodes (expect {expected}) ==")
    assert n == expected, "restore did not reproduce the backed-up state"
    print("PASS: per-store layout, real differential chain, non-destructive verify, restore")


if __name__ == "__main__":
    main()
