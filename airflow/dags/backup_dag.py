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
        def _all() -> list[str]:
            pol = load_policy(config.policy_path())
            return [f"{g.id}/{a}" for g in pol.groups_for_tier(tier_name) for a in g.names]

        if lane == "full":
            @task
            def targets() -> list[str]:
                return _all()

            @task(pool="neo4j_full")  # lanes: pool neo4j_full (1) / neo4j_diff (N)
            def backup(group_alias: str) -> dict:
                return backup_one(group_alias, "FULL")

            backup.expand(group_alias=targets())
            return

        # diff lane (#74): members whose current database has no full to diff against (just
        # seeded, or recreated by a by-name restore) get a FULL in the full pool instead
        @task
        def split() -> dict:
            return split_by_base(_all())

        @task
        def diff_targets(s: dict) -> list[str]:
            return s["diff"]

        @task
        def full_targets(s: dict) -> list[str]:
            return s["full"]

        @task(pool="neo4j_diff")
        def backup(group_alias: str) -> dict:
            return backup_one(group_alias, kind)

        @task(pool="neo4j_full")
        def backup_full(group_alias: str) -> dict:
            return backup_one(group_alias, "FULL")

        s = split()
        backup.expand(group_alias=diff_targets(s))
        backup_full.expand(group_alias=full_targets(s))

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
