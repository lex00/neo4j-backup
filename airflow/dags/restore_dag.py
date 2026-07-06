"""Restore DAG — group-aligned seed-from-URI then alias swap. Pure Cypher over Bolt
(no runner). Mirrors the Dagster `restore_group` job. Params drive group + PITR; the
seed fan-out is mapped, the swap is a single downstream barrier task.
"""

from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config
from neo4j_backup_core import naming, paths
from neo4j_backup_core.policy import load_policy


@dag(
    dag_id="neo4j_restore",
    schedule=None,  # manual: airflow dags trigger neo4j_restore -c '{"group_id":"demo"}'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "group_id": Param("demo", type="string"),
        "restore_until": Param(None, type=["null", "string"]),  # ISO-8601, needs a chain
    },
    tags=["neo4j-backup", "restore"],
)
def neo4j_restore():
    @task
    def plan() -> dict:
        ctx = get_current_context()
        return {
            "group_id": ctx["params"]["group_id"],
            "restore_until": ctx["params"]["restore_until"],
            "ts": naming.ts(),               # one timestamp for the whole group
        }

    @task
    def aliases(plan: dict) -> list[str]:
        # a top-level list return — Airflow can only .expand() over a whole XCom, not a key
        return list(load_policy(config.policy_path()).group(plan["group_id"]).aliases)

    @task
    def seed(plan: dict, alias: str) -> dict:
        store, neo = config.store(), config.neo4j()
        key = store.latest_artifact_key(paths.alias_prefix(plan["group_id"], alias))
        if not key:
            raise RuntimeError(f"no artifact for {plan['group_id']}/{alias} — back up first")
        group = load_policy(config.policy_path()).group(plan["group_id"])
        newdb = naming.physical(alias, plan["ts"])
        neo.seed_database(newdb, store.s3_uri(key), restore_until=plan["restore_until"],
                          topology=group.topology_for(alias))
        return {"alias": alias, "newdb": newdb}

    @task
    def swap(seeded: list[dict]):  # barrier: runs once, after every seed completes
        neo = config.neo4j()
        for s in seeded:
            neo.alter_alias(s["alias"], s["newdb"])
            print(f"alias {s['alias']} -> {s['newdb']}")

    p = plan()
    swap(seed.partial(plan=p).expand(alias=aliases(p)))


neo4j_restore_dag = neo4j_restore()  # the DAG object (Airflow discovers it in globals)
