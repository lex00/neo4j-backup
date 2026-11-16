# Dagster

The `neo4j_backup_dagster` code location runs the policy's backups, verification, retention,
loads and restores in Dagster. It uses the same core (`neo4j_backup_core`) as the CLI, the MCP
server and the [Airflow DAGs](../airflow/README.md), so policy, bucket layout and behaviour are
the same; pick the orchestrator you already run. Deploying it in production is on
[deploy/DEPLOY.md](deploy/DEPLOY.md).

## What it defines

The package is `definitions.py` (assets, jobs, schedules, sensor) and `resources.py` (the
Neo4j, object-store and runner resources). Everything else is in `neo4j_backup_core`.

Assets, partitioned by `group/member` (dynamic partitions kept in step with the policy by the
`reconcile_registry` sensor):

| Asset | What a materialization does |
|---|---|
| `backup` | A backup of the database the member's alias points at, into its folder in the bucket. Has an asset check, `backup_taken`, that warns when no backup was written (a diff with no full to build on, or a store whose id changed; the next tick takes a full). |
| `verify` | Copies the newest chain aside in the bucket, aggregates the copy and runs `neo4j-admin database check` on it, then deletes the copy. |
| `aggregate` | Collapses the member's chain into one full, in place. This removes point-in-time restore inside the chain; it runs only on tiers that set `aggregate_cron`. |

Unpartitioned assets: `prune` (retention), `metadata_export` and `system_backup` (per cluster).

Jobs: `backup_job`, `verify_job`, `aggregate_job`, `prune_job`, `dbms_metadata_job`,
`dbms_system_job`, `restore_group`, `metadata_restore`, `load_job`, `rollback_job`,
`cleanup_job`, `move_job` (restore into another cluster), `status_job` (fails when a target is
past its `rpo_minutes`).

Schedules, generated from the policy when the code location loads:

| Schedule | From | Requests |
|---|---|---|
| `<tier>_full`, `<tier>_diff` | `full_cron`, `diff_cron` | a backup run per member (the diff schedule asks for a full for members that need one) |
| `<tier>_verify`, `<tier>_aggregate` | `verify_cron` (default daily 06:00), `aggregate_cron` (no default) | a run per member |
| `<tier>_prune`, `<tier>_cleanup` | `prune_cron` (default Sundays 07:00), `cleanup_cron` | one run for the tier |
| `<tier>_rebuild` | `rebuild_cron` | a `load_job` run per `load.mode: rebuild` group |
| `<cluster>_dbms_metadata`, `<cluster>_dbms_system` | `dbms.metadata_cron`, `dbms.system_backup_cron` | one run per cluster |
| `status_check` | `dbms.status_cron` (default every 15 minutes) | `status_job` |

Schedules and the sensor start stopped unless the policy says `schedules: running`. The
schedules only request partitions the sensor has created, so turn the sensor on with them; a
schedule tick that finds members without partitions logs a warning saying so.

## Configuration

| What | Where |
|---|---|
| What to back up, when, and how long to keep it | the policy file ([POLICY.md](../POLICY.md)), named by `NEO4J_BACKUP_POLICY` |
| Clusters and stores beyond the default | the policy's `clusters:` and `storage:` |
| Default cluster, default store, runner, secrets | environment variables ([ENVIRONMENT.md](../ENVIRONMENT.md)) |
| Concurrency | your instance's `dagster.yaml` ([deploy/dagster.yaml](deploy/dagster.yaml)) |
| Registering the code location | `workspace.yaml`, Helm values or `dagster_cloud.yaml` ([deploy/DEPLOY.md](deploy/DEPLOY.md)) |

## Where neo4j-admin runs

Backups, verify and aggregate run `neo4j-admin` through Dagster Pipes:

- `RUNNER_MODE=subprocess` (default): on the run worker, which needs `neo4j-admin` (a JDK and
  the Neo4j Enterprise distribution of your cluster's version), a scratch volume at
  `SCRATCH_PATH` big enough for the largest full, network to the backup port (6362) and to
  the bucket.
- `RUNNER_MODE=k8s`: each command runs in its own pod (`PipesK8sClient`) with a fresh scratch
  volume. Install the `[k8s]` extra and set `RUNNER_IMAGE` and the other `RUNNER_*` variables.
  Validated on k3d.

Restores, aliases and privileges are Cypher over Bolt from the run worker; they need no
runner.

## Go-live checklist

1. Applications connect through aliases. For existing databases, see "Adopt existing
   databases" in [OPERATOR.md](../OPERATOR.md) (`neo4j-backup adopt`).
2. Write the policy (copy `policies/demo.yaml`) and check it: `neo4j-backup policy validate
   <file>`.
3. Build the code-location image and register it ([deploy/DEPLOY.md](deploy/DEPLOY.md)); set
   the environment on the code server and the run workers.
4. Merge [deploy/dagster.yaml](deploy/dagster.yaml) into the instance's `dagster.yaml` and set
   the `neo4j` pool limit.
5. Give the Neo4j servers read access (and `kms:Decrypt`) on the bucket: they fetch seeds
   themselves.
6. Turn on `reconcile_registry`, materialize `backup` for one member, then `verify`, then a
   restore of a test group.
7. Turn on the tier schedules and `status_check`, and alert on failed runs.

## Develop and test

```bash
pip install -e 'orchestrator[dev,azure,gcp,mcp]'
pytest orchestrator/tests/
NEO4J_PASSWORD=x NEO4J_BACKUP_POLICY=policies/demo.yaml \
  dagster definitions validate -m neo4j_backup_dagster.definitions
```

Smokes against the local stack ([STACK.md](../STACK.md)): `orchestrator/smoke_local.py`,
`smoke_verify.py`, `smoke_rebuild_dagster.py`, `smoke_k8s.py`, `smoke_k8s_load.py`.

`naming.py` must give the same names as `bootstrap/naming.sh`; `tests/test_naming_parity.py`
checks it.
