"""Validate the Airflow adapter over a REAL differential chain — full + diff — plus
point-in-time restore (seedRestoreUntil). The Airflow analogue of orchestrator/smoke_phase6
+ the PITR path. Subprocess runner; against the local stack (STACK.md).

Shape: FULL all gold aliases -> mutate acme-orders -> DIFF acme-orders (a real 2-artifact
chain) -> tip restore via the group DAG (reproduces base+2) -> PITR-seed acme-orders to a
timestamp between the two mutations and assert it lands at base+1 (seedRestoreUntil honored
the timestamp). PITR is exercised on the single chain-bearing db directly — a group restore
with restore_until would fail on the full-only aliases, which is by design.

    airflow/.venv/bin/python airflow/smoke_pitr.py
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
ALIASES = ["acme-orders", "acme-graph", "acme-audit"]


def _af(*args):
    subprocess.run([AF, *args], check=True, env=os.environ,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ok(run) -> bool:
    return str(getattr(run, "state", run)).split(".")[-1].lower() == "success"


def _wait_until_after(neo, db, t):
    """Block until the server clock is strictly past `t`, so the next write commits after it.
    A bounded condition poll, not a fixed sleep (each Bolt round-trip exceeds clock resolution)."""
    for _ in range(50):
        if neo.run_on(db, "RETURN datetime() > datetime($t) AS past", t=t)[0]["past"]:
            return
    raise RuntimeError(f"server clock did not advance past {t}")


def main() -> None:
    _af("db", "migrate")
    _af("pools", "set", "neo4j_full", "1", "full lane")
    _af("pools", "set", "neo4j_diff", "6", "diff lane")

    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import naming, paths
    import backup_dag as bd
    import restore_dag as rd

    store, neo = config.store(), config.neo4j()

    print("== clean prior chains + FULL backup of each gold alias ==")
    for a in ALIASES:
        store.delete_prefix(paths.alias_prefix("demo", a))
    phys = neo.alias_target("acme-orders")
    assert phys, "alias acme-orders has no target — run `just bootstrap` first"
    base = neo.count_nodes(phys)
    for a in ALIASES:
        bd.backup_one(f"demo/{a}", "FULL")

    print("== mutate acme-orders, bracket a PITR timestamp, mutate again ==")
    neo.run_on(phys, "CREATE (:Pitr {tag:'first'})")
    t_mid = neo.run_on(phys, "RETURN toString(datetime()) AS t")[0]["t"]  # server clock, after #1
    _wait_until_after(neo, phys, t_mid)  # ensure #2 commits strictly after t_mid
    neo.run_on(phys, "CREATE (:Pitr {tag:'second'})")
    tip = neo.count_nodes(phys)  # base + 2
    assert tip == base + 2, f"expected base+2 live nodes, got {tip}"

    print("== DIFF backup of acme-orders (forms the chain) ==")
    bd.backup_one("demo/acme-orders", "DIFF")
    prefix = paths.physical_prefix("demo", "acme-orders", phys)
    chain = store.list_artifacts(prefix)
    assert len(chain) == 2, f"expected full+diff chain, got {len(chain)}"
    print(f"   real chain: {len(chain)} artifacts (full+diff) in one physical prefix")

    print("== TIP restore via group DAG (whole chain) ==")
    run = rd.neo4j_restore_dag.test(run_conf={"group_id": "demo"})
    assert _ok(run), f"restore DAG state={getattr(run,'state',run)}"
    n_tip = neo.count_nodes("acme-orders")  # via alias -> restored physical
    assert n_tip == tip, f"tip restore {n_tip} != {tip} (base+2)"
    print(f"   tip restore OK ({n_tip} nodes == base+2)")

    print("== PITR seed to t_mid (seedRestoreUntil, single chain-bearing db) ==")
    diff_key = store.latest_artifact_key(prefix)
    pitr_phys = naming.physical("acme-orders", naming.ts())
    try:
        neo.seed_database(pitr_phys, store.s3_uri(diff_key), restore_until=t_mid)
        n_pitr = neo.count_nodes(pitr_phys)
        assert n_pitr == base + 1, f"PITR {n_pitr} != base+1 ({base + 1})"
        print(f"   PITR OK ({n_pitr} nodes == base+1 — landed between full and diff)")
    finally:
        neo.drop_database(pitr_phys)

    print("PASS: Airflow real differential chain, tip restore, and PITR (seedRestoreUntil) validated")


if __name__ == "__main__":
    main()
