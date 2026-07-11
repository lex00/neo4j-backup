"""Backup DAGs — one per (tier, lane), fanning out over the policy's (group, alias).

Mirrors the Dagster `backup` asset: back up the physical DB each alias currently targets,
into its per-store prefix `<group>/<slug>/<physical>/`. neo4j-admin is the only non-Cypher
step; it runs via the execution dispatcher (subprocess or k8s pod per RUNNER_MODE — see
neo4j_backup_airflow.execution). Airflow 3.x Task SDK + dynamic mapping.
"""

from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import load_policy

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


def backup_one(group_alias: str, kind: str) -> dict:
    """Resolve the alias's live physical and back it up to its per-store prefix — via the shared
    `neo4j_backup_core.ops.backup_target` (same code path as the Dagster asset and the CLI)."""
    group_id, alias = group_alias.split("/", 1)
    neo, store, runner = config.neo4j(), config.store(), config.runner()
    try:
        return ops.backup_target(run_admin, neo, store, runner, _layout, group_id, alias, kind,
                                 upload=upload.BACKUP_UPLOAD, staging=upload.STAGING)
    except ops.OpError as e:
        raise RuntimeError(str(e))


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
            return [f"{g.id}/{a}" for g in pol.groups_for_tier(tier_name) for a in g.names]

        @task(pool=f"neo4j_{lane}")  # lanes: pool neo4j_full (1) / neo4j_diff (N)
        def backup(group_alias: str) -> dict:
            return backup_one(group_alias, kind)

        backup.expand(group_alias=targets())

    return _backup()


# Generate one DAG per (tier, lane) from the policy, at module scope (Airflow discovers
# DAGs in module globals). Try/except so a missing/unreachable policy doesn't break DAG
# parsing — and so an s3:// source works (os.path.exists would be False for it) (#43).
try:
    _pol = load_policy(config.policy_path())
except Exception:  # noqa: BLE001 — no policy yet / source unreachable; skip DAG generation
    _pol = None
if _pol is not None:
    for _tier, _t in _pol.tiers.items():
        globals()[f"neo4j_backup_{_tier}_full"] = make_backup_dag(_tier, "full", _t.full_cron, "FULL")
        globals()[f"neo4j_backup_{_tier}_diff"] = make_backup_dag(_tier, "diff", _t.diff_cron, "DIFF")
