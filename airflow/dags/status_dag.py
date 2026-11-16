"""Freshness against rpo_minutes (#123, #233): `neo4j_status`, on the policy's
`dbms.status_cron`, fails when any active target is stale, so Airflow's failure notifications
(on_failure_callback, email, your alerting) cover it. Read-only: the bucket's listings."""

import os
from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config
from neo4j_backup_core import freshness, paths
from neo4j_backup_core.policy import load_policy, load_policy_at_startup

_layout = paths.get_layout()


def make_status_dag(cron: str):
    @dag(dag_id="neo4j_status", schedule=cron, start_date=datetime(2025, 1, 1), catchup=False,
         max_active_runs=1, tags=["neo4j-backup", "status"])
    def _status():
        @task
        def check() -> dict:
            pol = load_policy(config.policy_path())
            out = freshness.status(config.store(), _layout,
                                   [g for g in pol.db_groups if not g.retired])
            print(f"{len(out['targets'])} targets, {len(out['stale'])} stale")
            if out["stale"]:
                raise RuntimeError(f"{len(out['stale'])} target(s) past rpo_minutes: "
                                   f"{', '.join(out['stale'])}")
            return {"targets": len(out["targets"]), "stale": out["stale"]}

        check()

    return _status()


_pol = load_policy_at_startup(config.policy_path(), configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None and _pol.dbms.status_cron:
    neo4j_status = make_status_dag(_pol.dbms.status_cron)
