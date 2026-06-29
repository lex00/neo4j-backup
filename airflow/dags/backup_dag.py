"""Backup DAGs — one per (tier, lane), fanning out over the policy's (group, alias).

Mirrors the Dagster `backup` asset: back up the physical DB each alias currently targets,
into its per-store prefix `<group>/<slug>/<physical>/`. neo4j-admin is the only non-Cypher
step; it runs via the execution dispatcher (subprocess or k8s pod per RUNNER_MODE — see
neo4j_backup_airflow.execution). Airflow 3.x Task SDK + dynamic mapping.
"""

import os
from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import paths
from neo4j_backup_core.policy import load_policy


def backup_one(group_alias: str, kind: str) -> dict:
    """Resolve the alias's live physical and back it up to its per-store prefix."""
    group_id, alias = group_alias.split("/", 1)
    neo, store, runner = config.neo4j(), config.store(), config.runner()
    physical = neo.alias_target(alias)
    if not physical:
        raise RuntimeError(f"alias {alias!r} has no target — bootstrap the group first")
    prefix = paths.physical_prefix(group_id, alias, physical)
    cmd = runner.backup_command(physical, store.s3_uri(prefix), kind=kind)
    run_admin(cmd)  # subprocess or k8s pod per RUNNER_MODE; non-zero exit -> fail
    artifact = store.latest_artifact_key(prefix)
    return {"group": group_id, "alias": alias, "physical": physical, "artifact": artifact}


def make_backup_dag(tier_name: str, lane: str, cron: str, kind: str):
    @dag(
        dag_id=f"neo4j_backup_{tier_name}_{lane}",
        schedule=cron,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["neo4j-backup", "backup", lane, tier_name],
    )
    def _backup():
        @task
        def targets() -> list[str]:
            pol = load_policy(config.policy_path())
            return [f"{g.id}/{a}" for g in pol.groups_for_tier(tier_name) for a in g.aliases]

        @task(pool=f"neo4j_{lane}")  # lanes: pool neo4j_full (1) / neo4j_diff (N)
        def backup(group_alias: str) -> dict:
            return backup_one(group_alias, kind)

        backup.expand(group_alias=targets())

    return _backup()


# Generate one DAG per (tier, lane) from the policy, at module scope (Airflow discovers
# DAGs in module globals). Guard so a missing policy doesn't break DAG parsing.
if os.path.exists(config.policy_path()):
    _pol = load_policy(config.policy_path())
    for _tier, _t in _pol.tiers.items():
        globals()[f"neo4j_backup_{_tier}_full"] = make_backup_dag(_tier, "full", _t.full_cron, "FULL")
        globals()[f"neo4j_backup_{_tier}_diff"] = make_backup_dag(_tier, "diff", _t.diff_cron, "DIFF")
