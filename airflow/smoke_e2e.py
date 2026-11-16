"""Validate the Airflow backup + restore DAGs end to end via dag.test() (in-process),
against the local stack (STACK.md). The Airflow analogue of the Dagster smokes.

    airflow/.venv/bin/python airflow/smoke_e2e.py
"""

import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T", "runner"]
os.environ.update({
    "AIRFLOW_HOME": os.path.join(REPO, ".airflow_home"),
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "AIRFLOW__CORE__DAGS_FOLDER": os.path.join(REPO, "airflow", "dags"),
    "NEO4J_BACKUP_POLICY": os.path.join(REPO, "policies", "demo.yaml"),
    "NEO4J_BOLT_URI": "neo4j://localhost:7687",
    "NEO4J_PASSWORD": "devpassword",
    "BACKUP_BUCKET": "neo4j-backups",
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "NEO4J_BACKUP_SOURCE": "neo4j:6362",
    "RUNNER_EXEC_PREFIX": json.dumps(EXEC),
    "RUNNER_PAGECACHE": "512M",
    "RUNNER_HEAP_SIZE": "2G",
})

AF = os.path.join(REPO, "airflow", ".venv", "bin", "airflow")


def _af(*args):
    subprocess.run([AF, *args], check=True, env=os.environ,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ok(run) -> bool:
    return str(getattr(run, "state", run)).split(".")[-1].lower() == "success"


def main() -> None:
    print("== init Airflow metadata DB + lane pools ==")
    _af("db", "migrate")
    _af("pools", "set", "neo4j_full", "1", "full backup lane")
    _af("pools", "set", "neo4j_diff", "6", "diff backup lane")

    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import paths
    import avp_dags as av
    import backup_dag as bd
    import restore_dag as rd

    store, neo = config.store(), config.neo4j()
    phys = neo.alias_target("acme-orders")
    assert phys, "alias acme-orders has no target — run `just bootstrap`"
    prefix = paths.physical_prefix("demo", "acme-orders", phys)
    before = len(store.list_artifacts(prefix))

    print("== BACKUP via dag.test() (full lane, gold tier) ==")
    run = bd.neo4j_backup_gold_full.test()
    assert _ok(run), f"backup DAG state={getattr(run,'state',run)}"
    after = len(store.list_artifacts(prefix))
    assert after > before, f"no artifact written ({before}->{after})"
    print(f"   backup OK ({before}->{after} artifacts)")

    print("== VERIFY via dag.test() (non-destructive) ==")
    pre = len(store.list_artifacts(paths.alias_prefix("demo", "acme-orders")))
    run = av.neo4j_verify_gold.test()
    assert _ok(run), f"verify DAG state={getattr(run,'state',run)}"
    post = len(store.list_artifacts(paths.alias_prefix("demo", "acme-orders")))
    assert post == pre, f"verify mutated prod artifacts ({pre}->{post})"
    assert not store.list_artifacts("_verify/"), "verify scratch not cleaned"
    print("   verify OK (consistent, prod chain intact, scratch cleaned)")

    from neo4j_backup_core import artifacts

    def types_at(alias):
        p = neo.alias_target(alias)
        metas, _u = artifacts.read(store, paths.physical_prefix("demo", alias, p))
        return p, sorted(m["type"] for m in metas.values())

    expected = neo.count_nodes(phys)
    print("== RESTORE via dag.test(run_conf=group_id=demo), with the baseline (#97) ==")
    run = rd.neo4j_restore_dag.test(run_conf={"group_id": "demo", "ticket": "CHG-SMOKE",
                                              "reason": "airflow smoke"})
    assert _ok(run), f"restore DAG state={getattr(run,'state',run)}"
    n = neo.count_nodes("acme-orders")
    assert n == expected, f"restored {n} != expected {expected}"
    newphys, types = types_at("acme-orders")
    assert types == ["full"], types
    print(f"   restore OK ({n} nodes via alias); baseline full of {newphys}")

    print("== the next diff-lane run takes a diff (#97) ==")
    assert bd.split_by_base(["demo/acme-orders"])["diff"] == ["demo/acme-orders"]
    neo.run_on(newphys, "CREATE (:AfterRestore)")
    run = bd.neo4j_backup_gold_diff.test()
    assert _ok(run), f"diff DAG state={getattr(run,'state',run)}"
    assert types_at("acme-orders")[1] == ["diff", "full"], types_at("acme-orders")
    print("   diff OK")

    print("== RESTORE with baseline=false, then the DIFF lane (#74) ==")
    run = rd.neo4j_restore_dag.test(run_conf={"group_id": "demo", "baseline": False})
    assert _ok(run), f"restore DAG state={getattr(run,'state',run)}"
    newphys, types = types_at("acme-orders")
    assert types == [], types
    assert bd.split_by_base(["demo/acme-orders"]) == {"diff": [], "full": ["demo/acme-orders"]}
    run = bd.neo4j_backup_gold_diff.test()
    assert _ok(run), f"diff DAG state={getattr(run,'state',run)}"
    assert types_at("acme-orders")[1] == ["full"], types_at("acme-orders")
    print(f"   diff DAG sent acme-orders to the full pool; {newphys} now has a full")

    print("== PRUNE via dag.test() ==")
    run = av.neo4j_prune_gold.test()
    assert _ok(run), f"prune DAG state={getattr(run,'state',run)}"
    print("   prune OK")

    print("PASS: Airflow backup + verify + restore + prune validated end to end (dag.test)")


if __name__ == "__main__":
    sys.exit(main())
