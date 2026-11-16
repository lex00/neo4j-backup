"""DBMS metadata DAGs (#14) — agentless logical backup/restore of the security + alias
layer (users, roles, privileges, aliases) as replayable Cypher. Pure Cypher over Bolt +
object store, no runner. See neo4j_backup_core.metadata.

- neo4j_dbms_metadata_default: capture -> render -> store one `_dbms/metadata-<ts>.cypher`, on
  the policy's `dbms.metadata_cron` (#104), then the DBMS sweep.
- neo4j_metadata_restore: replay the latest (or a given `key`) against `system` over Bolt.
"""

import os
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from neo4j_backup_airflow import config
from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import load_policy, load_policy_at_startup

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


def make_metadata_dag(cluster: str, cron: str):
    """`neo4j_dbms_metadata_<cluster>` on the policy's `dbms.metadata_cron` (#104); then the
    DBMS sweep keeps `dbms.keep_metadata` / `keep_system`."""
    @dag(dag_id=f"neo4j_dbms_metadata_{cluster}", schedule=cron, start_date=datetime(2025, 1, 1),
         catchup=False, max_active_runs=1, tags=["neo4j-backup", "metadata", cluster])
    def _metadata():
        @task
        def export() -> str:
            key = ops.export_metadata(config.neo4j(), config.store(), _layout,
                                      cluster=cluster)["key"]
            swept = ops.prune(config.store(), load_policy(config.policy_path()), _layout,
                              groups=[], cluster=cluster)
            print(f"metadata export -> {key}; dbms sweep deleted {swept['deleted']}")
            return key

        export()

    return _metadata()


_pol = load_policy_at_startup(config.policy_path(), configured="NEO4J_BACKUP_POLICY" in os.environ)
if _pol is not None:
    for _cl in _pol.cluster_names():  # one per cluster (#106)
        globals()[f"neo4j_dbms_metadata_{_cl}"] = make_metadata_dag(_cl, _pol.dbms.metadata_cron)


@dag(dag_id="neo4j_metadata_restore", schedule=None, start_date=datetime(2025, 1, 1),
     catchup=False, params={"key": Param(None, type=["null", "string"]),  # default: latest
             "cluster": Param("default", type="string")},  # #106
     tags=["neo4j-backup", "metadata", "restore"])
def neo4j_metadata_restore():
    @task
    def restore() -> dict:
        try:
            params = get_current_context()["params"]
            result = ops.restore_metadata(config.neo4j(), config.store(), _layout,
                                          params["key"] or None,
                                          cluster=params.get("cluster") or "default")
        except ops.OpError as e:
            raise RuntimeError(str(e))
        print(f"replayed {result['applied']} statements from {result['key']}; "
              f"skipped {len(result['skipped'])}")
        return result

    restore()


neo4j_metadata_restore_dag = neo4j_metadata_restore()
