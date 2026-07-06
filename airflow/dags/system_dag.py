"""System-database binary backup DAG (#15) — exact metadata backup (native passwords,
roles, privileges, catalog) for native-auth teams. Companion to the agentless logical
export (#14): this captures the binary `system` store via neo4j-admin so it can be restored
*exactly*.

Restore is offline + node-local (path B) — `system` cannot be seed-from-URI'd or stopped —
so there is no restore DAG; use `bootstrap/restore_system.sh` / `just restore-system`. FULL
only: `system` is tiny and a standalone full per run avoids chain/aggregation entirely.
"""

from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import paths

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


@dag(dag_id="neo4j_system_backup", schedule="0 1 * * *", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "system"])
def neo4j_system_backup():
    @task
    def backup() -> str:
        store, runner = config.store(), config.runner()
        prefix = _layout.system_prefix()
        run_admin(runner.backup_command("system", store.s3_uri(prefix), kind="FULL"))
        key = store.latest_artifact_key(prefix)
        print(f"system backup -> {key}")
        return key

    backup()


neo4j_system_backup_dag = neo4j_system_backup()
