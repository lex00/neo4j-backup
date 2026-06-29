"""Scaffold DAG — proves the Airflow adapter loads the policy and the core wiring.

Airflow 3.x: author with the Task SDK (`from airflow.sdk import ...`). The @dag-decorated
function must be called at module scope so Airflow discovers it.
"""

from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config
from neo4j_backup_core.policy import load_policy


@dag(
    schedule=None,                       # manual trigger only (3.x: `schedule`, not `schedule_interval`)
    start_date=datetime(2025, 1, 1),
    catchup=False,                       # default in 3.x, set explicitly
    tags=["neo4j-backup"],
)
def neo4j_backup_scaffold():
    @task
    def list_targets():
        # policy load only — no Neo4j/S3 connection (safe at parse via lazy task exec)
        pol = load_policy(config.policy_path())
        keys = pol.partition_keys()
        print("backup targets (group/alias):", keys)
        return keys

    list_targets()


neo4j_backup_scaffold()
