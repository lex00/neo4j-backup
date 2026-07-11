"""Restore DAG — pure Cypher over Bolt (no runner). Mirrors the Dagster `restore_group` job.
Params drive group + PITR (+ replace for by-name); the seed fan-out is mapped, the barrier
does the alias-swap cutover (or nothing in by-name mode).

Modes (per group, from policy `restore_mode`): alias-swap seeds a fresh physical per alias and
swaps the alias to it (non-destructive, default); by-name (#48) restores each database into its
own name — create-if-absent, or DROP+recreate an existing one with `replace=true` (destructive;
Neo4j has no rename). In by-name each mapped seed validates its own artifact before its own
drop; the Dagster op additionally pre-validates the whole group before any drop.
"""

import os
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config
from neo4j_backup_core import cutover, naming, paths
from neo4j_backup_core.policy import load_policy

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()
# Cypher language for the seed statement (unset = Cypher-25 default; "5" for a Cypher-5 cluster)
_SEED_CYPHER_VERSION = os.environ.get("SEED_CYPHER_VERSION") or None


@dag(
    dag_id="neo4j_restore",
    schedule=None,  # manual: airflow dags trigger neo4j_restore -c '{"group_id":"demo"}'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "group_id": Param("demo", type="string"),
        "restore_until": Param(None, type=["null", "string"]),  # ISO-8601, needs a chain
        "replace": Param(False, type="boolean"),  # by-name only: DROP+recreate an existing target
    },
    tags=["neo4j-backup", "restore"],
)
def neo4j_restore():
    @task
    def plan() -> dict:
        ctx = get_current_context()
        gid = ctx["params"]["group_id"]
        return {
            "group_id": gid,
            "restore_until": ctx["params"]["restore_until"],
            "replace": bool(ctx["params"].get("replace")),
            "mode": load_policy(config.policy_path()).group(gid).restore_mode,
            "ts": naming.ts(),               # one timestamp for the whole group
        }

    @task
    def members(plan: dict) -> list[str]:
        # a top-level list return — Airflow can only .expand() over a whole XCom, not a key
        return list(load_policy(config.policy_path()).group(plan["group_id"]).names)

    @task
    def seed(plan: dict, name: str) -> dict:
        store, neo = config.store(), config.neo4j()
        group = load_policy(config.policy_path()).group(plan["group_id"])
        key = store.latest_artifact_key(_layout.alias_prefix(plan["group_id"], name))
        if not key:
            raise RuntimeError(f"no artifact for {plan['group_id']}/{name} — back up first")
        if plan["mode"] == "by-name":
            if neo.database_exists(name):
                if not plan["replace"]:
                    raise RuntimeError(f"database {name!r} exists; set replace=true to DROP+recreate (destructive)")
                neo.drop_database(name)
            neo.seed_database(name, store.s3_uri(key), restore_until=plan["restore_until"],
                              topology=group.topology_for(name), cypher_version=_SEED_CYPHER_VERSION)
            return {"name": name, "mode": "by-name"}
        old = neo.alias_target(name)  # captured before cutover (for external routing #17)
        newdb = naming.physical(name, plan["ts"])
        neo.seed_database(newdb, store.s3_uri(key), restore_until=plan["restore_until"],
                          topology=group.topology_for(name), cypher_version=_SEED_CYPHER_VERSION)
        return {"alias": name, "newdb": newdb, "old": old, "mode": "alias-swap"}

    @task
    def swap(seeded: list[dict]):  # barrier: runs once, after every seed completes
        neo = config.neo4j()
        strategy = cutover.from_env()  # alias-swap (default) or external routing (#17)
        for s in seeded:
            if s.get("mode") == "by-name":
                print(f"restored {s['name']}")
                continue
            strategy.cutover(neo, s["alias"], s["newdb"], s.get("old"))
            print(f"cutover {s['alias']} -> {s['newdb']}")

    p = plan()
    swap(seed.partial(plan=p).expand(name=members(p)))


neo4j_restore_dag = neo4j_restore()  # the DAG object (Airflow discovers it in globals)
