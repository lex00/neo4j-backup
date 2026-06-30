"""DBMS metadata DAGs (#14) — agentless logical backup/restore of the security + alias
layer (users, roles, privileges, aliases) as replayable Cypher. Pure Cypher over Bolt +
object store, no runner. See neo4j_backup_core.metadata.

- neo4j_metadata_backup: capture -> render -> store one `_dbms/metadata-<ts>.cypher`.
- neo4j_metadata_restore: replay the latest (or a given `key`) against `system` over Bolt.
"""

from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config
from neo4j_backup_core import metadata, naming, paths


@dag(dag_id="neo4j_metadata_backup", schedule="0 2 * * *", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "metadata"])
def neo4j_metadata_backup():
    @task
    def export() -> str:
        neo, store = config.neo4j(), config.store()
        ts = naming.ts()
        key = paths.metadata_key(ts)
        store.put_text(key, metadata.render(metadata.capture(neo), ts=ts))
        return key

    export()


@dag(dag_id="neo4j_metadata_restore", schedule=None, start_date=datetime(2025, 1, 1),
     catchup=False, params={"key": Param(None, type=["null", "string"])},  # default: latest
     tags=["neo4j-backup", "metadata", "restore"])
def neo4j_metadata_restore():
    @task
    def restore() -> dict:
        neo, store = config.neo4j(), config.store()
        key = get_current_context()["params"]["key"] or store.latest_text_key(paths.metadata_prefix())
        if not key:
            raise RuntimeError("no metadata artifact — run neo4j_metadata_backup first")
        result = metadata.replay(neo, store.get_text(key))
        print(f"replayed {result['applied']} statements from {key}; skipped {len(result['skipped'])}")
        return result

    restore()


neo4j_metadata_backup_dag = neo4j_metadata_backup()
neo4j_metadata_restore_dag = neo4j_metadata_restore()
