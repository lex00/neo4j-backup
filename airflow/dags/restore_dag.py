"""Restore DAG — the seed and cutover are pure Cypher over Bolt. Mirrors the Dagster
`restore_group` job. Params drive group + PITR (+ replace for by-name, + baseline); the seed
fan-out is mapped, the barrier does the alias-swap cutover (or nothing in by-name mode), and a
mapped `baseline` task in the `neo4j_full` pool then takes a FULL of each restored database
with the runner (#97).

Modes (per group, from policy `restore_mode`): alias-swap seeds a fresh physical per alias and
swaps the alias to it (non-destructive, default); by-name (#48) restores each database into its
own name — create-if-absent, or DROP+recreate an existing one with `replace=true` (destructive;
Neo4j has no rename). In by-name each mapped seed validates its own artifact before its own
drop; the Dagster op additionally pre-validates the whole group before any drop.
"""

import os
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import naming, ops, paths, record
from neo4j_backup_core.policy import load_policy
from neo4j_backup_core.until import parse_until

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
        "baseline": Param(True, type="boolean"),  # FULL of each restored database afterwards (#97)
        "from_load": Param(False, type="boolean"),  # newest load artifact per member (#144)
        "from_dump": Param({}, type="object"),  # member -> load key or external dump URI (#159)
        "members": Param([], type="array"),  # restore only these members (#121)
        "ticket": Param(None, type=["null", "string"]),  # change ticket, recorded (#119)
        "reason": Param(None, type=["null", "string"]),
    },
    tags=["neo4j-backup", "restore"],
)
def neo4j_restore():
    @task
    def plan() -> dict:
        ctx = get_current_context()
        gid = ctx["params"]["group_id"]
        until = parse_until(ctx["params"]["restore_until"])
        rec = record.fields(ctx["params"].get("ticket"), ctx["params"].get("reason"),
                            applying=True)  # also in the DAG run's conf (#119)
        print(record.describe("restore", rec))
        # every member's seed (and the chain covering `until`) is chosen here, so a time no
        # chain covers fails before any seed task runs (#114)
        loads = ctx["params"].get("from_dump") or ("latest" if ctx["params"].get("from_load")
                                                   else None)
        ops.choose_seeds(config.store(), _layout,
                         ops.only_members(load_policy(config.policy_path()).group(gid),
                                          ctx["params"].get("members")), until, loads)
        return {
            "group_id": gid,
            # ISO-8601, UTC if no offset; a bad value fails here, before any Neo4j call (#77)
            "restore_until": parse_until(ctx["params"]["restore_until"]),
            "replace": bool(ctx["params"].get("replace")),
            "baseline": bool(ctx["params"].get("baseline", True)),
            "loads": loads,
            "members": list(ctx["params"].get("members") or []),
            **rec,
            "mode": load_policy(config.policy_path()).group(gid).restore_mode,
            "ts": naming.ts(),               # one timestamp for the whole group
        }

    @task
    def members(plan: dict) -> list[str]:
        # a top-level list return — Airflow can only .expand() over a whole XCom, not a key
        group = load_policy(config.policy_path()).group(plan["group_id"])
        return list(ops.only_members(group, plan.get("members")).names)  # (#121)

    @task
    def seed(plan: dict, name: str) -> dict:
        # Per-member seed via the shared core op — Airflow maps this across tasks (parallel), so
        # each validates its own artifact before its own drop (the Dagster/CLI path pre-validates
        # the whole group). One group-wide timestamp (plan["ts"]) keeps alias-swap physicals aligned.
        group = load_policy(config.policy_path()).group(plan["group_id"])
        try:
            loads = plan.get("loads")
            seed = ops.choose_seed(config.store(), _layout, group.id, name,
                                   plan["restore_until"],
                                   loads.get(name) if isinstance(loads, dict) else loads)
            return ops.seed_member(config.neo4j(), config.store(), group, _layout, name, seed=seed,
                                   restore_until=plan["restore_until"], replace=plan["replace"],
                                   cypher_version=_SEED_CYPHER_VERSION, ts=plan["ts"], log=print)
        except ops.OpError as e:
            raise RuntimeError(str(e))

    @task
    def swap(seeded: list[dict]) -> list[dict]:  # barrier: runs once, after every seed completes
        seeded = list(seeded)
        ops.cutover_seeded(config.neo4j(), seeded, log=print, store=config.store())
        return seeded

    @task(pool="neo4j_full")  # a full backup: the full lane's pool (#97)
    def baseline(plan: dict, member: dict) -> dict | None:
        """A FULL of one restored database, after the cutover. A failure fails this task only;
        the restore is already applied (rerun the task or `backup --kind FULL`)."""
        if not plan["baseline"]:
            print("baseline skipped (baseline=false)")
            return None
        group = load_policy(config.policy_path()).group(plan["group_id"])
        try:
            return ops.baseline_after_restore(
                run_admin, config.neo4j(), config.store(), config.runner(), _layout, group,
                [member], upload=upload.BACKUP_UPLOAD, staging=upload.STAGING, log=print)[0]
        except ops.BaselineError as e:
            raise RuntimeError(f"[{e.kind}] {e}")

    p = plan()
    restored = swap(seed.partial(plan=p).expand(name=members(p)))
    baseline.partial(plan=p).expand(member=restored)


neo4j_restore_dag = neo4j_restore()  # the DAG object (Airflow discovers it in globals)


@dag(dag_id="neo4j_rollback", schedule=None, start_date=datetime(2025, 1, 1), catchup=False,
     params={"group_id": Param("demo", type="string"), "members": Param([], type="array"),
             "ticket": Param(None, type=["null", "string"]),
             "reason": Param(None, type=["null", "string"])},
     tags=["neo4j-backup", "restore", "rollback"])
def neo4j_rollback():
    @task
    def rollback() -> dict:
        p = get_current_context()["params"]
        rec = record.fields(p.get("ticket"), p.get("reason"), applying=True)
        print(record.describe("rollback", rec))
        group = load_policy(config.policy_path()).group(p["group_id"])
        try:
            out = ops.rollback(config.neo4j(), config.store(), group,
                               members=list(p.get("members") or []) or None, log=print)
        except ops.OpError as e:
            raise RuntimeError(str(e))
        return {**out, **rec}

    rollback()


neo4j_rollback_dag = neo4j_rollback()
