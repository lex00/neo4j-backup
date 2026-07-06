"""Aggregate / Verify / Prune DAGs. Mirror the Dagster assets.

- aggregate: collapse a physical's chain into one recovered full, in place (RTO/retention).
- verify: NON-destructive consistency check — copy the chain to a scratch prefix,
  aggregate the copy, `neo4j-admin database check` it, then delete the copy (try/finally).
- prune: age-based retention (boto3 only), keep the chain head.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from neo4j_backup_airflow import config
from neo4j_backup_airflow.execution import run_admin
from neo4j_backup_core import metadata, paths

# storage-key layout instance (#21) — swappable via PATH_LAYOUT
_layout = paths.get_layout()
from neo4j_backup_core.policy import load_policy


def aggregate_one(group_alias: str) -> dict:
    group_id, alias = group_alias.split("/", 1)
    store, runner = config.store(), config.runner()
    head = store.latest_artifact_key(_layout.alias_prefix(group_id, alias))
    if not head:
        raise RuntimeError(f"no artifact for {group_id}/{alias}")
    physical = _layout.physical_of_key(group_id, alias, head)
    prefix = _layout.physical_prefix(group_id, alias, physical)
    run_admin(runner.aggregate_command(physical, store.s3_uri(prefix)))
    return {"physical": physical, "full": store.latest_artifact_key(prefix)}


def verify_one(group_alias: str) -> dict:
    group_id, alias = group_alias.split("/", 1)
    store, runner = config.store(), config.runner()
    head = store.latest_artifact_key(_layout.alias_prefix(group_id, alias))
    if not head:
        raise RuntimeError(f"no artifact for {group_id}/{alias}")
    physical = _layout.physical_of_key(group_id, alias, head)
    src = _layout.physical_prefix(group_id, alias, physical)
    scratch = f"_verify/{group_id}/{physical}/"
    try:
        store.copy_prefix(src, scratch)
        run_admin(runner.aggregate_command(physical, store.s3_uri(scratch)))
        full = store.latest_artifact_key(scratch)
        run_admin(runner.check_command(physical, store.s3_uri(full)))
    finally:
        store.delete_prefix(scratch)
    return {"alias": alias, "physical": physical, "consistent": True}


def prune_all() -> int:
    pol = load_policy(config.policy_path())
    store = config.store()
    now = datetime.now(timezone.utc)
    deleted = 0
    for g in pol.db_groups:
        cutoff = now - timedelta(days=g.retention_days)
        for a in g.aliases:
            arts = store.list_artifacts(_layout.alias_prefix(g.id, a))
            if not arts:
                continue
            newest = max(arts, key=lambda t: t[2])[0]  # keep the chain head
            stale = [k for (k, _s, m) in arts if m < cutoff and k != newest]
            deleted += store.delete_keys(stale)
    deleted += metadata.prune(store)  # keep the newest N DBMS metadata exports
    sysarts = sorted(store.list_artifacts(_layout.system_prefix()), key=lambda t: t[2])
    deleted += store.delete_keys([k for (k, _s, _m) in sysarts[:-14]])  # keep newest 14 system fulls
    return deleted


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
