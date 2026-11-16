"""Backup DAGs — one per (tier, lane), fanning out over the policy's (group, alias).

Mirrors the Dagster `backup` asset: back up the physical DB each alias currently targets,
into its per-store prefix `<group>/<slug>/<physical>/`. neo4j-admin is the only non-Cypher
step; it runs via the execution dispatcher (subprocess or k8s pod per RUNNER_MODE — see
neo4j_backup_airflow.execution). Airflow 3.x Task SDK + dynamic mapping.
"""

import os
from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import load_policy, load_policy_at_startup

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


def backup_one(group_alias: str, kind: str) -> dict:
    """Resolve the alias's live physical and back it up to its per-store prefix — via the shared
    `neo4j_backup_core.ops.backup_target` (same code path as the Dagster asset and the CLI)."""
    group_id, alias = group_alias.split("/", 1)
    neo, store, runner = config.neo4j(), config.store(), config.runner()
    try:
        return ops.backup_target(run_admin, neo, store, runner, _layout, group_id, alias, kind,
                                 upload=upload.BACKUP_UPLOAD, staging=upload.STAGING, log=print)
    except ops.OpError as e:
        raise RuntimeError(str(e))


def split_by_base(keys: list[str]) -> dict:
    """{"diff": [...], "full": [...]}: which members can take a diff and which need a full
    first (#74). A failed check leaves the member in the diff list; backup_one then skips it."""
    neo, store = config.neo4j(), config.store()
    out: dict[str, list[str]] = {"diff": [], "full": []}
    for key in keys:
        group_id, alias = key.split("/", 1)
        try:
            full = ops.needs_full(neo, store, _layout, group_id, alias)
        except Exception as e:  # noqa: BLE001
            print(f"{key}: base check failed ({e}); requesting the diff")
            full = False
        out["full" if full else "diff"].append(key)
    return out


def pool_for(cluster: str, lane: str) -> str:
    """The Airflow pool for a cluster's lane (#106): `neo4j_<lane>` for the default cluster (as
    before), `neo4j_<cluster>_<lane>` for a named one."""
    return f"neo4j_{lane}" if cluster == "default" else f"neo4j_{cluster}_{lane}"


def make_backup_dag(tier_name: str, lane: str, cron: str, kind: str,
                    clusters: list[str] | None = None):
    clusters = clusters or ["default"]

    @dag(
        dag_id=f"neo4j_backup_{tier_name}_{lane}",
        schedule=cron,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["neo4j-backup", "backup", lane, tier_name],
    )
    def _backup():
        def _all(cluster: str) -> list[str]:
            pol = load_policy(config.policy_path())
            return [f"{g.id}/{a}" for g in pol.groups_for_tier(tier_name)
                    if (g.cluster or "default") == cluster for a in g.names]

        for cl in clusters:  # one set of tasks per cluster, in that cluster's pools (#106)
            sfx = "" if cl == "default" else f"_{cl}"

            if lane == "full":
                @task(task_id=f"targets{sfx}")
                def targets(cl=cl) -> list[str]:
                    return _all(cl)

                @task(task_id=f"backup{sfx}", pool=pool_for(cl, "full"))
                def backup(group_alias: str) -> dict:
                    return backup_one(group_alias, "FULL")

                backup.expand(group_alias=targets())
                continue

            # diff lane (#74): members whose current database has no full to diff against
            # (just seeded, or recreated by a by-name restore) get a FULL in the full pool instead
            @task(task_id=f"split{sfx}")
            def split(cl=cl) -> dict:
                return split_by_base(_all(cl))

            @task(task_id=f"diff_targets{sfx}")
            def diff_targets(s: dict) -> list[str]:
                return s["diff"]

            @task(task_id=f"full_targets{sfx}")
            def full_targets(s: dict) -> list[str]:
                return s["full"]

            @task(task_id=f"backup{sfx}", pool=pool_for(cl, "diff"))
            def backup_diff(group_alias: str) -> dict:
                return backup_one(group_alias, kind)

            @task(task_id=f"backup_full{sfx}", pool=pool_for(cl, "full"))
            def backup_full(group_alias: str) -> dict:
                return backup_one(group_alias, "FULL")

            sp = split()
            backup_diff.expand(group_alias=diff_targets(sp))
            backup_full.expand(group_alias=full_targets(sp))

    return _backup()


# Generate one DAG per (tier, lane) from the policy, at module scope (Airflow discovers
# DAGs in module globals). A policy that fails to load raises here, so it shows as an import
# error in the DagBag instead of as missing DAGs (#75); transient fetch errors are retried first.
# Only "no policy configured at all" generates nothing.
_pol = load_policy_at_startup(config.policy_path(),
                              configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None:
    for _tier, _t in _pol.tiers.items():
        # a load-only tier may have neither cron (#103)
        _cls = sorted({g.cluster or "default" for g in _pol.groups_for_tier(_tier)}) or ["default"]
        if _t.full_cron:
            globals()[f"neo4j_backup_{_tier}_full"] = make_backup_dag(
                _tier, "full", _t.full_cron, "FULL", _cls)
        if _t.diff_cron:
            globals()[f"neo4j_backup_{_tier}_diff"] = make_backup_dag(
                _tier, "diff", _t.diff_cron, "DIFF", _cls)
