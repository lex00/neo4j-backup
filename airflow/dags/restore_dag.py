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

from neo4j_backup_airflow import alerts, config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import naming, ops, paths, record
from neo4j_backup_core.policy import load_policy
from neo4j_backup_core.until import parse_until

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()
# Cypher language for the seed statement (unset = Cypher-25 default; "5" for a Cypher-5 cluster)
_SEED_CYPHER_VERSION = os.environ.get("SEED_CYPHER_VERSION") or None


@dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
    dag_id="neo4j_restore",
    schedule=None,  # manual: airflow dags trigger neo4j_restore -c '{"group_id":"demo"}'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "group_id": Param(type="string"),  # required: no default group (#235)
        "dry_run": Param(False, type="boolean"),  # log the plan and stop; nothing changes (#249)
        "restore_until": Param(None, type=["null", "string"]),  # ISO-8601, needs a chain
        "check": Param(False, type="boolean"),  # the group's load.checks before the swap (#221)
        "keep_failed": Param(False, type="boolean"),  # with check: keep new dbs on a failure
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
        # chosen once for the whole group, so a "latest" restore is aligned (#225)
        seeds = ops.choose_seeds(config.store(), _layout,
                                 ops.only_members(load_policy(config.policy_path()).group(gid),
                                                  ctx["params"].get("members")), until, loads)
        point = next((sd.get("point") for sd in seeds.values() if sd.get("point")), None)
        if point:
            print(f"restore point: {point}")
        if ctx["params"].get("dry_run"):  # the CLI's --dry-run, in Airflow (#249)
            import json
            from airflow.exceptions import AirflowSkipException
            group = ops.only_members(load_policy(config.policy_path()).group(gid),
                                     ctx["params"].get("members"))
            preview = ops.plan_restore(config.neo4j(), config.store(), group, _layout,
                                       replace=bool(ctx["params"].get("replace")),
                                       until=until, loads=loads)
            print("restore plan (dry run, nothing changed):")
            print(json.dumps(preview, indent=1, default=str))
            raise AirflowSkipException("dry run: plan logged above; nothing changed")
        return {
            "group_id": gid,
            # ISO-8601, UTC if no offset; a bad value fails here, before any Neo4j call (#77)
            "restore_until": parse_until(ctx["params"]["restore_until"]),
            "replace": bool(ctx["params"].get("replace")),
            "baseline": bool(ctx["params"].get("baseline", True)),
            "loads": loads,
            "members": list(ctx["params"].get("members") or []),
            "seeds": seeds,
            "check": bool(ctx["params"].get("check")),
            "keep_failed": bool(ctx["params"].get("keep_failed")),
            **rec,
            "mode": load_policy(config.policy_path()).group(gid).restore_mode,
            "ts": naming.ts_for(load_policy(config.policy_path()).group(gid)),  # #88
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
            seed = plan["seeds"][name]  # chosen in the plan, for the whole group (#225)
            return ops.seed_member(config.neo4j(), config.store(), group, _layout, name, seed=seed,
                                   restore_until=plan["restore_until"], replace=plan["replace"],
                                   cypher_version=_SEED_CYPHER_VERSION, ts=plan["ts"], log=print)
        except ops.OpError as e:
            raise RuntimeError(str(e))

    @task
    def swap(plan: dict, seeded: list[dict]) -> list[dict]:  # barrier: after every seed
        seeded = list(seeded)
        if plan.get("check"):  # the policy's checks on the new databases first (#111, #221)
            group = load_policy(config.policy_path()).group(plan["group_id"])
            try:
                ops.check_seeded(config.neo4j(), group, seeded,
                                 keep_failed=plan.get("keep_failed", False),
                                 require_data=False, log=print)
            except ops.OpError as e:
                raise RuntimeError(str(e))
        ops.cutover_seeded(config.neo4j(), seeded, log=print, store=config.store())
        return seeded

    # a full backup: the full lane's pool (#97). The group is a run parameter and Airflow fixes
    # a task's pool at parse time, so this is `neo4j_full` whatever the group's cluster (#232).
    @task(pool="neo4j_full")
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
    restored = swap(p, seed.partial(plan=p).expand(name=members(p)))
    baseline.partial(plan=p).expand(member=restored)


neo4j_restore_dag = neo4j_restore()  # the DAG object (Airflow discovers it in globals)


@dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id="neo4j_rollback", schedule=None, start_date=datetime(2025, 1, 1), catchup=False,
     params={"group_id": Param(type="string"), "members": Param([], type="array"),
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


@dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id="neo4j_restore_to_cluster", schedule=None, start_date=datetime(2025, 1, 1),
     catchup=False,
     params={"to_cluster": Param("default", type="string"),
             "group_id": Param(None, type=["null", "string"]),
             "from_cluster": Param(None, type=["null", "string"]),
             "until": Param(None, type=["null", "string"]),
             "fence_source": Param(False, type="boolean"),
             "skip_metadata": Param(False, type="boolean"),
             "baseline": Param(True, type="boolean"),
             "ticket": Param(None, type=["null", "string"]),
             "reason": Param(None, type=["null", "string"])},
     tags=["neo4j-backup", "restore", "move"])
def neo4j_restore_to_cluster():
    """Restore a group, or every group of a cluster, into another cluster (#115)."""
    @task
    def move_groups() -> dict:
        from neo4j_backup_core import move

        p = get_current_context()["params"]
        rec = record.fields(p.get("ticket"), p.get("reason"), applying=True)
        print(record.describe("restore --to-cluster", rec))
        pol = load_policy(config.policy_path())
        try:
            groups, source = move.groups_for(pol, p.get("group_id"), p.get("from_cluster"))
            out = move.public(move.move(
                config.neo4j(), config.store(), _layout, groups, source, p["to_cluster"],
                until=parse_until(p.get("until")), fence_source=p.get("fence_source", False),
                skip_metadata=p.get("skip_metadata", False), clusters=pol.cluster_names(),
                log=print))
            if p.get("baseline", True):
                out["baseline"] = move.baseline(run_admin, config.neo4j(), config.store(),
                                                config.runner(), _layout, groups, out, log=print,
                                                upload=upload.BACKUP_UPLOAD,
                                                staging=upload.STAGING)
        except ops.OpError as e:
            raise RuntimeError(str(e))
        print(out["policy"])
        return {**out, **rec}

    move_groups()


neo4j_restore_to_cluster_dag = neo4j_restore_to_cluster()
