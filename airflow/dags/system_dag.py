"""System-database binary backup DAG (#15) — exact metadata backup (native passwords,
roles, privileges, catalog) for native-auth teams. Companion to the agentless logical
export (#14): this captures the binary `system` store via neo4j-admin so it can be restored
*exactly*.

Restore is offline + node-local (path B) — `system` cannot be seed-from-URI'd or stopped —
so there is no restore DAG; use `bootstrap/restore_system.sh` / `just restore-system`. FULL
only: `system` is tiny and a standalone full per run avoids chain/aggregation entirely.
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


def make_system_dag(cluster: str, cron: str):
    """`neo4j_dbms_system_<cluster>` on the policy's `dbms.system_backup_cron` (#104); then the
    DBMS sweep keeps `dbms.keep_system` / `keep_metadata`."""
    @dag(default_args=dict(alerts.DEFAULT_ARGS),  # failure alerts (#246)
     dag_id=f"neo4j_dbms_system_{cluster}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1, tags=["neo4j-backup", "system", cluster])
    def _system():
        @task
        def backup() -> str:
            store = config.store()
            # admin (direct s3://) or pipeline (local + boto3 upload) per BACKUP_UPLOAD
            key = ops.system_backup(run_admin, store, config.runner(), _layout,
                                    upload=upload.BACKUP_UPLOAD, staging=upload.STAGING,
                                    cluster=cluster)["key"]
            swept = ops.prune(store, load_policy(config.policy_path()), _layout, groups=[],
                              cluster=cluster)
            print(f"system backup -> {key}; dbms sweep deleted {swept['deleted']}")
            return key

        backup()

    return _system()


_pol = load_policy_at_startup(config.policy_path(), configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None:
    for _cl in _pol.dbms_cluster_names():  # one per cluster in use (#106, #251)
        globals()[f"neo4j_dbms_system_{_cl}"] = make_system_dag(_cl, _pol.dbms.system_backup_cron)
