"""Aggregate / Verify / Prune DAGs. Mirror the Dagster assets.

- aggregate: collapse a physical's chain into one recovered full, in place (RTO/retention).
- verify: NON-destructive consistency check — copy the chain to a scratch prefix,
  aggregate the copy, `neo4j-admin database check` it, then delete the copy (try/finally).
- prune: age-based retention (boto3 only), keep the chain head.
"""

from datetime import datetime

from airflow.sdk import dag, task

from neo4j_backup_airflow import config, upload
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import load_policy

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()


def aggregate_one(group_alias: str) -> dict:
    group_id, alias = group_alias.split("/", 1)
    try:
        return ops.aggregate_target(run_admin, config.store(), config.runner(), _layout,
                                    group_id, alias, upload=upload.BACKUP_UPLOAD,
                                    staging=upload.STAGING)
    except ops.OpError as e:
        raise RuntimeError(str(e))


def verify_one(group_alias: str) -> dict:
    group_id, alias = group_alias.split("/", 1)
    try:
        return ops.verify_target(run_admin, config.store(), config.runner(), _layout,
                                 group_id, alias, upload=upload.BACKUP_UPLOAD,
                                 staging=upload.STAGING)
    except ops.OpError as e:
        raise RuntimeError(str(e))


def prune_all() -> int:
    return ops.prune(config.store(), load_policy(config.policy_path()), _layout)["deleted"]


def _targets() -> list[str]:
    return load_policy(config.policy_path()).partition_keys()


@dag(dag_id="neo4j_aggregate", schedule="0 5 * * 0", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "retention"])
def neo4j_aggregate():
    @task
    def targets() -> list[str]:
        return _targets()

    @task
    def aggregate(group_alias: str) -> dict:
        return aggregate_one(group_alias)

    aggregate.expand(group_alias=targets())


@dag(dag_id="neo4j_verify", schedule="0 6 * * *", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "verify"])
def neo4j_verify():
    @task
    def targets() -> list[str]:
        return _targets()

    @task
    def verify(group_alias: str) -> dict:
        return verify_one(group_alias)

    verify.expand(group_alias=targets())


@dag(dag_id="neo4j_prune", schedule="0 7 * * 0", start_date=datetime(2025, 1, 1),
     catchup=False, tags=["neo4j-backup", "retention"])
def neo4j_prune():
    @task
    def prune() -> int:
        return prune_all()

    prune()


neo4j_aggregate_dag = neo4j_aggregate()
neo4j_verify_dag = neo4j_verify()
neo4j_prune_dag = neo4j_prune()
