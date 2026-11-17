# Dagster

The `neo4j_backup_dagster` code location runs the policy's jobs in Dagster. It shares
`neo4j_backup_core` with the CLI, the MCP server and the [Airflow DAGs](../airflow/README.md).
To run it in production, see [deploy/DEPLOY.md](deploy/DEPLOY.md).

## What it defines

Assets partitioned by `group/member`, kept in step with the policy by the `reconcile_registry` sensor:

| Asset | A materialization |
|---|---|
| `backup` | Backs up the member's database. The `backup_taken` check warns when nothing was written. |
| `verify` | Aggregates a copy of the newest chain, runs `neo4j-admin database check` on it, deletes it. |
| `aggregate` | Collapses the chain into one full in place, removing point-in-time restore inside it. |

Unpartitioned assets: `prune`, `metadata_export` and `system_backup` (per cluster).

| Jobs | |
|---|---|
| `backup_job`, `verify_job`, `aggregate_job` | per member |
| `prune_job`, `cleanup_job` | per tier |
| `dbms_metadata_job`, `dbms_system_job`, `metadata_restore` | per cluster |
| `restore_group`, `rollback_job`, `load_job` | per group |
| `move_job` | restore into another cluster |
| `status_job` | fails when a target is past `rpo_minutes` |

Schedules are built from the policy when the location loads:

| Schedule | Policy key |
|---|---|
| `<tier>_full`, `<tier>_diff` | `full_cron`, `diff_cron` |
| `<tier>_verify` | `verify_cron` (default daily 06:00) |
| `<tier>_aggregate` | `aggregate_cron` (no default) |
| `<tier>_prune`, `<tier>_cleanup` | `prune_cron` (default Sundays 07:00), `cleanup_cron` |
| `<tier>_rebuild` | `rebuild_cron`, for `load.mode: rebuild` groups |
| `<cluster>_dbms_metadata`, `<cluster>_dbms_system` | `dbms.metadata_cron`, `dbms.system_backup_cron` |
| `status_check` | `dbms.status_cron` (default every 15 minutes) |

Schedules and `reconcile_registry` start stopped unless the policy says `schedules: running`.
Turn them on together: schedules only request partitions the sensor has created.

## Configuration

| What | Where |
|---|---|
| Groups, tiers, crons, retention | the policy ([POLICY.md](../POLICY.md)), named by `NEO4J_BACKUP_POLICY` |
| More clusters and stores | the policy's `clusters:` and `storage:` |
| Default cluster and store, runner, secrets | [ENVIRONMENT.md](../ENVIRONMENT.md) |
| Concurrency | [deploy/dagster.yaml](deploy/dagster.yaml) |
| Registering the location | [deploy/DEPLOY.md](deploy/DEPLOY.md) |

`neo4j-admin` runs through Dagster Pipes on the run worker or in a pod per command
([Choosing a runner mode](../ENVIRONMENT.md#choosing-a-runner-mode)). Restores and aliases are
Cypher over Bolt.

## Go live

1. Make applications connect through aliases ([Adopt existing databases](../workflows/onboard.md#adopt-existing-databases)).
2. Write the policy (copy `policies/demo.yaml`) and run `neo4j-backup policy validate <file>`.
3. Deploy as in [deploy/DEPLOY.md](deploy/DEPLOY.md).
4. Give the Neo4j servers read access (and `kms:Decrypt`) on the bucket.
5. Turn on `reconcile_registry`. Back up and verify one member, then restore a test group.
6. Turn on the tier schedules and `status_check`, and alert on failed runs.

## Develop and test

```bash
pip install -e 'orchestrator[dev,azure,gcp,mcp]'
pytest orchestrator/tests/
NEO4J_PASSWORD=x NEO4J_BACKUP_POLICY=policies/demo.yaml \
  dagster definitions validate -m neo4j_backup_dagster.definitions
```

Smoke scripts against the [local stack](../STACK.md) are `orchestrator/smoke_*.py`, for example
`smoke_local.py` and `smoke_k8s.py`.
`tests/test_naming_parity.py` checks that `naming.py` matches `bootstrap/naming.sh`.
