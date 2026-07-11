"""DBMS metadata DAGs (#14) — agentless logical backup/restore of the security + alias
layer (users, roles, privileges, aliases) as replayable Cypher. Pure Cypher over Bolt +
object store, no runner. See neo4j_backup_core.metadata.

- neo4j_metadata_backup: capture -> render -> store one `_dbms/metadata-<ts>.cypher`.
- neo4j_metadata_restore: replay the latest (or a given `key`) against `system` over Bolt.
"""

from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config
from neo4j_backup_core import ops, paths

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


@dag(dag_id="neo4j_metadata_backup", schedule="0 2 * * *", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "metadata"])
def neo4j_metadata_backup():
    @task
    def export() -> str:
        return ops.export_metadata(config.neo4j(), config.store(), _layout)["key"]

    export()


@dag(dag_id="neo4j_metadata_restore", schedule=None, start_date=datetime(2025, 1, 1),
     catchup=False, params={"key": Param(None, type=["null", "string"])},  # default: latest
     tags=["neo4j-backup", "metadata", "restore"])
def neo4j_metadata_restore():
    @task
    def restore() -> dict:
        try:
            result = ops.restore_metadata(config.neo4j(), config.store(), _layout,
                                          get_current_context()["params"]["key"] or None)
        except ops.OpError as e:
            raise RuntimeError(str(e))
        print(f"replayed {result['applied']} statements from {result['key']}; "
              f"skipped {len(result['skipped'])}")
        return result

    restore()


neo4j_metadata_backup_dag = neo4j_metadata_backup()
neo4j_metadata_restore_dag = neo4j_metadata_restore()
