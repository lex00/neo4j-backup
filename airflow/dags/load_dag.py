"""Load / rebuild DAGs (#107, #108).

- neo4j_load: manual (`-c '{"group_id": "catalog"}'`), one group: build in the runner, dump, seed,
  check, set access, swap, baseline. A failure is a failed run naming the step; the previous load
  keeps serving.
- neo4j_rebuild_<tier>: on the tier's `rebuild_cron`, one mapped task per `load.mode: rebuild`
  group of the tier.

Each load takes the group's lock (`neo4j_backup_core.locks`), which the backup DAGs also take,
so a group's rebuild never overlaps its own backups or another rebuild (Airflow pools can't
express per-group exclusion).
"""

import os
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import load as load_mod
from neo4j_backup_core import locks, paths, record
from neo4j_backup_core.policy import load_policy, load_policy_at_startup

_layout = paths.get_layout()
LOCK_WAIT = int(os.environ.get("NEO4J_BACKUP_LOCK_WAIT", "3600"))
_CYPHER = os.environ.get("SEED_CYPHER_VERSION") or None


def load_one(group_id: str, replace: bool = False, keep_failed: bool = False,
             baseline: bool = True) -> dict:
    group = load_policy(config.policy_path()).group(group_id)
    store = config.store()
    with locks.group_lock(store, group_id, "load", wait=LOCK_WAIT):
        try:
            return load_mod.load_group(
                run_admin, config.runner(), config.neo4j(), store, _layout, group,
                replace=replace, keep_failed=keep_failed, baseline=None if baseline else False,
                upload=upload.BACKUP_UPLOAD, staging=upload.STAGING, cypher_version=_CYPHER,
                log=print)
        except load_mod.LoadError as e:
            raise RuntimeError(f"[{e.step}] {e}")


@dag(dag_id="neo4j_load", schedule=None, start_date=datetime(2025, 1, 1), catchup=False,
     params={"group_id": Param("demo", type="string"),
             "replace": Param(False, type="boolean"),
             "keep_failed": Param(False, type="boolean"),
             "baseline": Param(True, type="boolean"),
             "ticket": Param(None, type=["null", "string"]),
             "reason": Param(None, type=["null", "string"])},
     tags=["neo4j-backup", "load"])
def neo4j_load():
    # the group is a run parameter, and Airflow fixes a task's pool when the DAG is parsed, so a
    # manual load uses the default cluster's full pool whatever its cluster (#232)
    @task(pool="neo4j_full")
    def load() -> dict:
        p = get_current_context()["params"]
        rec = record.fields(p.get("ticket"), p.get("reason"), applying=True)
        print(record.describe("load", rec))
        out = load_one(p["group_id"], bool(p.get("replace")), bool(p.get("keep_failed")),
                       bool(p.get("baseline", True)))
        return {**out, **rec}

    load()


neo4j_load_dag = neo4j_load()


def make_rebuild_dag(tier: str, cron: str, clusters: list[str] | None = None):
    """`neo4j_rebuild_<tier>`: one mapped task per cluster (#232), each in that cluster's full
    pool (`neo4j_full`, or `neo4j_<cluster>_full` for a named cluster, as the backups use)."""
    pool_for = config.pool_for

    clusters = clusters or ["default"]

    @dag(dag_id=f"neo4j_rebuild_{tier}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1, tags=["neo4j-backup", "rebuild", tier])
    def _rebuild():
        @task
        def groups(cluster: str) -> list[str]:
            pol = load_policy(config.policy_path())
            return [g.id for g in pol.groups_for_tier(tier)
                    if g.load.mode == "rebuild" and (g.cluster or "default") == cluster]

        @task
        def rebuild(group_id: str) -> dict:
            return load_one(group_id)

        for cluster in clusters:
            rebuild.override(task_id=f"rebuild_{cluster}", pool=pool_for(cluster, "full")).expand(
                group_id=groups.override(task_id=f"groups_{cluster}")(cluster))

    return _rebuild()


_pol = load_policy_at_startup(config.policy_path(), configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None:
    for _tier, _t in _pol.tiers.items():
        if _t.rebuild_cron:
            _cls = sorted({g.cluster or "default" for g in _pol.groups_for_tier(_tier)
                           if g.load.mode == "rebuild"}) or ["default"]
            globals()[f"neo4j_rebuild_{_tier}"] = make_rebuild_dag(_tier, _t.rebuild_cron, _cls)
