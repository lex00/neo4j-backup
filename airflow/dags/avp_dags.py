"""Aggregate / Verify / Prune DAGs, one of each per tier, on the tier's crons (#104). Mirror
the Dagster `{tier}_verify` / `{tier}_aggregate` / `{tier}_prune` schedules.

- neo4j_aggregate_<tier>: collapse each member's chain into one recovered full, in place.
- neo4j_verify_<tier>: NON-destructive consistency check — copy the chain to a scratch prefix,
  aggregate the copy, `neo4j-admin database check` it, then delete the copy (try/finally).
- neo4j_prune_<tier>: chain-aware retention over the tier's groups (#73).
"""

import os
from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import alerts, config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import load_policy, load_policy_at_startup

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


LOCK_WAIT = int(os.environ.get("NEO4J_BACKUP_LOCK_WAIT", "3600"))


def aggregate_one(group_alias: str) -> dict:
    from neo4j_backup_core import locks
    group_id, alias = group_alias.split("/", 1)
    try:  # never with the group's backups or rebuild (#223)
        with locks.group_lock(config.store(), group_id, f"aggregate {alias}", wait=LOCK_WAIT):
            return ops.aggregate_target(run_admin, config.store(), config.runner(), _layout,
                                        group_id, alias, upload=upload.BACKUP_UPLOAD,
                                        staging=upload.STAGING, log=print, skip_missing=True)
    except (ops.OpError, locks.Locked) as e:
        raise RuntimeError(str(e))


def verify_one(group_alias: str) -> dict:
    from neo4j_backup_core import locks
    group_id, alias = group_alias.split("/", 1)
    try:
        with locks.group_lock(config.store(), group_id, f"verify {alias}", wait=LOCK_WAIT):
            return ops.verify_target(run_admin, config.store(), config.runner(), _layout,
                                     group_id, alias, upload=upload.BACKUP_UPLOAD,
                                     staging=upload.STAGING, skip_missing=True, log=print)
    except (ops.OpError, locks.Locked) as e:
        raise RuntimeError(str(e))


def prune_tier(tier: str) -> int:
    """Chain-aware prune (#73) of one tier's groups; the DBMS sweep runs with the DBMS DAGs
    (#104). Artifacts without a sidecar are inspected first via run_admin."""
    pol = load_policy(config.policy_path())
    return ops.prune(config.store(), pol, _layout, groups=pol.groups_for_tier(tier, include_retired=True), dbms=False,
                     run_admin=run_admin, runner=config.runner(), log=print)["deleted"]


def _tier_targets(tier: str, cluster: str | None = None) -> list[str]:
    pol = load_policy(config.policy_path())
    return [f"{g.id}/{a}" for g in pol.groups_for_tier(tier) for a in g.names
            if cluster is None or (g.cluster or "default") == cluster]


def make_member_dag(tier: str, lane: str, cron: str, clusters: list[str] | None = None):
    """`neo4j_verify_<tier>` / `neo4j_aggregate_<tier>`: one mapped task per member (#104), per
    cluster in that cluster's full pool (#250): each needs scratch the size of a full."""
    one = verify_one if lane == "verify" else aggregate_one
    clusters = clusters or ["default"]

    @dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id=f"neo4j_{lane}_{tier}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1,
         tags=["neo4j-backup", "verify" if lane == "verify" else "retention", tier])
    def _member():
        @task
        def targets(cluster: str) -> list[str]:
            return _tier_targets(tier, cluster)

        @task
        def run(group_alias: str) -> dict:
            return one(group_alias)

        for cluster in clusters:
            suffix = "" if clusters == ["default"] else f"_{cluster}"
            run.override(task_id=f"{lane}{suffix}", pool=config.pool_for(cluster, "full")).expand(
                group_alias=targets.override(task_id=f"targets{suffix}")(cluster))

    return _member()


def make_prune_dag(tier: str, cron: str):
    @dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id=f"neo4j_prune_{tier}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1, tags=["neo4j-backup", "retention", tier])
    def _prune():
        @task
        def prune() -> int:
            return prune_tier(tier)

        prune()

    return _prune()


def make_cleanup_dag(tier: str, cron: str):
    """`neo4j_cleanup_<tier>` on the tier's `cleanup_cron` (#93): drop superseded databases."""
    @dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id=f"neo4j_cleanup_{tier}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1, tags=["neo4j-backup", "cleanup", tier])
    def _cleanup():
        @task
        def cleanup() -> int:
            pol = load_policy(config.policy_path())
            t = pol.tiers[tier]  # keep and soak period from the tier (#231)
            return sum(len(ops.cleanup(config.neo4j(), g, keep=t.cleanup_keep,
                                       older_than_days=t.cleanup_older_than_days,
                                       log=print)["drops"])
                       for g in pol.groups_for_tier(tier))

        cleanup()

    return _cleanup()


# One DAG per tier and job, from the policy's crons (#104); the old fixed neo4j_verify /
# neo4j_aggregate / neo4j_prune DAGs are gone. A policy that fails to load is an import error.
_pol = load_policy_at_startup(config.policy_path(), configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None:
    for _tier, _t in _pol.tiers.items():
        _cls = sorted({g.cluster or "default" for g in _pol.groups_for_tier(_tier)}) or ["default"]
        if _t.verify_cron:
            globals()[f"neo4j_verify_{_tier}"] = make_member_dag(_tier, "verify", _t.verify_cron,
                                                                 _cls)
        if _t.aggregate_cron:
            globals()[f"neo4j_aggregate_{_tier}"] = make_member_dag(_tier, "aggregate",
                                                                    _t.aggregate_cron, _cls)
        if _t.prune_cron:
            globals()[f"neo4j_prune_{_tier}"] = make_prune_dag(_tier, _t.prune_cron)
        if _t.cleanup_cron:
            globals()[f"neo4j_cleanup_{_tier}"] = make_cleanup_dag(_tier, _t.cleanup_cron)
