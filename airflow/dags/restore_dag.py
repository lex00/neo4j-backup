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
from neo4j_backup_core import naming, ops, paths
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
        # Per-member seed via the shared core op — Airflow maps this across tasks (parallel), so
        # each validates its own artifact before its own drop (the Dagster/CLI path pre-validates
        # the whole group). One group-wide timestamp (plan["ts"]) keeps alias-swap physicals aligned.
        group = load_policy(config.policy_path()).group(plan["group_id"])
        try:
            return ops.seed_member(config.neo4j(), config.store(), group, _layout, name,
                                   restore_until=plan["restore_until"], replace=plan["replace"],
                                   cypher_version=_SEED_CYPHER_VERSION, ts=plan["ts"], log=print)
        except ops.OpError as e:
            raise RuntimeError(str(e))

    @task
    def swap(seeded: list[dict]):  # barrier: runs once, after every seed completes
        ops.cutover_seeded(config.neo4j(), seeded, log=print)

    p = plan()
    swap(seed.partial(plan=p).expand(name=members(p)))


neo4j_restore_dag = neo4j_restore()  # the DAG object (Airflow discovers it in globals)
