# Airflow

Airflow 3 DAGs for the policy's backups, verification, retention, loads and restores. They use
the same core (`neo4j_backup_core`) as the CLI, the MCP server and the
[Dagster code location](../orchestrator/README.md), so policy, bucket layout and behaviour are
the same; pick the orchestrator you already run. Validated with Airflow 3.2.2.

## The DAGs

DAG files are in `airflow/dags/`; the shared code they import (`neo4j_backup_airflow`: the
environment, `run_admin` for subprocess or Kubernetes pods, upload helpers) is in
`orchestrator/neo4j_backup_airflow/`. The scheduled DAGs are generated from the policy when the
DAG file is parsed.

| DAG | Schedule | What it does |
|---|---|---|
| `neo4j_backup_<tier>_full`, `neo4j_backup_<tier>_diff` | the tier's `full_cron`, `diff_cron` | Back up every member of the tier. The diff DAG takes a full of members that need one. A skipped backup (no full to build on) shows as a skipped task. |
| `neo4j_verify_<tier>` | `verify_cron` (default daily 06:00) | Copy each member's newest chain aside in the bucket, aggregate the copy, `neo4j-admin database check`, delete the copy. |
| `neo4j_aggregate_<tier>` | `aggregate_cron` (no default) | Collapse each chain into one full, in place; removes point-in-time restore inside the chain. |
| `neo4j_prune_<tier>` | `prune_cron` (default Sundays 07:00) | Delete backups past `retention_days`, whole chains only. |
| `neo4j_cleanup_<tier>` | `cleanup_cron` | Drop superseded databases the tool created (keeps the tier's `cleanup_keep`, and anything younger than `cleanup_older_than_days`). |
| `neo4j_rebuild_<tier>` | `rebuild_cron` | Load each `load.mode: rebuild` group of the tier. |
| `neo4j_dbms_metadata_<cluster>`, `neo4j_dbms_system_<cluster>` | `dbms.metadata_cron`, `dbms.system_backup_cron` | Export users, roles, privileges and aliases as Cypher; binary backup of `system`. |
| `neo4j_status` | `dbms.status_cron` (default every 15 minutes) | Fails when any target is past its `rpo_minutes`. Alert on its failures. |
| `neo4j_restore` | manual | Restore a group (Params below). |
| `neo4j_rollback` | manual | Point a group's aliases back at their previous databases. Params: `group_id`, `members`, `ticket`, `reason`. |
| `neo4j_restore_to_cluster` | manual | Restore groups into another cluster. Params: `to_cluster`, `group_id` or `from_cluster`, `until`, `fence_source`, `skip_metadata`, `baseline`, `ticket`, `reason`. |
| `neo4j_load` | manual | Load or rebuild one group. Params: `group_id`, `replace`, `keep_failed`, `baseline`, `ticket`, `reason`. |
| `neo4j_metadata_restore` | manual | Replay a metadata export. Params: `cluster`, `key`. |
| `neo4j_backup_scaffold` | manual | Loads the policy and prints the targets; for checking a deployment. |

A tier that has no groups still gets its DAGs. Members of a named cluster run in the pools
`neo4j_<cluster>_full` / `neo4j_<cluster>_diff` (the default cluster: `neo4j_full` /
`neo4j_diff`), and the rebuild DAGs use the cluster's full pool. The manual restore and load
DAGs always use `neo4j_full` for their baseline and load tasks: the group is a run parameter
and Airflow fixes a task's pool when it parses the DAG.

`neo4j_restore` Params:

| Param | Default | |
|---|---|---|
| `group_id` | required | the group |
| `restore_until` | none | ISO-8601 time (UTC if no offset); without it the group goes back to the newest instant every member covers |
| `members` | `[]` | only these members |
| `check` | `false` | run the group's `load.checks` on the new databases before the swap |
| `keep_failed` | `false` | with `check`: keep the new databases when a check fails |
| `replace` | `false` | by-name groups: drop and recreate an existing database |
| `from_load` | `false` | seed each member from its newest load |
| `from_dump` | `{}` | `{member: key or URI}` |
| `baseline` | `true` | take a full backup of each new database afterwards |
| `ticket`, `reason` | none | recorded in the log and the result |

Triggering it applies the restore: there's no preview in Airflow. Preview with the CLI
(`neo4j-backup restore <group> --dry-run`, `neo4j-backup restore-points <group>`; see below).

## Deploying in production

### Install into your Airflow image

The Python package (`neo4j-backup-dagster`) lists Dagster as a dependency; install it without
dependencies and add the core's own:

```bash
pip install --no-deps \
  "neo4j-backup-dagster @ git+https://github.com/lex00/neo4j-backup@v0.6.0#subdirectory=orchestrator"
pip install "neo4j>=5" "boto3>=1.34" "pydantic>=2" "pyyaml>=6" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt"
# providers: cncf.kubernetes for RUNNER_MODE=k8s; azure-storage-blob / google-cloud-storage
# for CLOUD=azure / gcp
```

Copy `airflow/dags/*.py` into the deployment's DAGs folder (they import only
`neo4j_backup_airflow` and `neo4j_backup_core` from the package).

### Environment and policy

Everything is environment variables ([ENVIRONMENT.md](../ENVIRONMENT.md)); Airflow
Connections and Variables aren't used. Set them on the scheduler, the DAG processor and every
worker. The DAG processor builds the DAGs from the policy, so it must read
`NEO4J_BACKUP_POLICY`: use an `s3://` path (the DAG processor then needs read access to it) or
a file mounted at the same path everywhere. Always set it: the default `policies/demo.yaml` is
relative to the process's working directory. A policy that doesn't load makes every
policy-generated DAG file show an import error.

### Pools

```bash
airflow pools set neo4j_full 1 "full backups, verifies, aggregates, loads"
airflow pools set neo4j_diff 6 "differential backups"
# per named cluster: neo4j_<cluster>_full and neo4j_<cluster>_diff
```

Each DAG run also takes a per-group lock in the bucket (`_locks/<group>.lock`), so a member's
backup, verify, aggregate and its group's rebuild never overlap; a task waits up to
`NEO4J_BACKUP_LOCK_WAIT` seconds for it.

### Running neo4j-admin

- `RUNNER_MODE=subprocess` (default): neo4j-admin runs on the worker. The worker image then
  needs a JDK and the Neo4j Enterprise distribution of your cluster's version, a scratch
  volume at `SCRATCH_PATH` big enough for the largest full, and network to the backup port
  (6362) and the bucket.
- `RUNNER_MODE=k8s`: each command runs in its own pod (`KubernetesPodOperator`, started from
  inside the task). Set `RUNNER_IMAGE` (the Neo4j Enterprise image), `RUNNER_NAMESPACE`
  (default `default`), and the sizes: `RUNNER_SCRATCH_STORAGE` (default `6Ti` per pod),
  `RUNNER_MEMORY_LIMIT` (default `4Gi`), `RUNNER_CPU_REQUEST`, `RUNNER_MEMORY_REQUEST`,
  `RUNNER_TOLERATIONS`, `RUNNER_NODE_SELECTOR`. The worker's service account needs to create,
  watch and delete pods, read pod logs and create PVCs in that namespace (a Role with `pods`,
  `pods/log`, `pods/exec` and `persistentvolumeclaims`, bound to the worker's service account).
  The pod image needs `sh`, `mkdir`, `printf` and `base64` (the Neo4j image has them). Give
  the pods cloud credentials through IRSA or workload identity (`RUNNER_SERVICE_ACCOUNT`), or
  map secrets with `RUNNER_SECRET_ENV`; credentials are never written into a pod spec as plain
  text. `BACKUP_UPLOAD=pipeline` is refused in this mode.

Restores, aliases and privileges are Cypher over Bolt from the worker.

### Turn it on

1. Check the deployment: trigger `neo4j_backup_scaffold`; it prints the policy's targets.
2. Unpause `neo4j_backup_<tier>_full` for one tier and trigger it; then `neo4j_verify_<tier>`.
3. Unpause the remaining scheduled DAGs you use, and `neo4j_status`. Set an
   `on_failure_callback` or email on failures (for example in a cluster policy), so a failed
   backup and a stale target reach someone.
4. Unpause the manual DAGs too (`neo4j_restore`, `neo4j_rollback`, `neo4j_load`, ...): a
   triggered run of a paused DAG waits until it's unpaused.

### The operator CLI host

Previews and read-only checks are CLI commands: `restore --dry-run`, `restore-points`,
`status`, `targets`, and backing up a single group (the Airflow backup DAGs run per tier). Run
them from a host or pod with the package, the same environment, and for `backup`/`verify`,
`neo4j-admin` and scratch (the worker image works: `kubectl exec` into a worker, or run a pod
from it). See [OPERATOR.md](../OPERATOR.md).

## Run it locally

Against the Compose stack (`just fresh`):

```bash
just airflow-install      # its own venv (uv, Python 3.13) with pinned Airflow
just airflow-smoke        # backup, verify, restore, prune (dag.test, in-process)
just airflow-pitr         # a real full + diff chain and a point-in-time restore
just airflow-k8s-smoke    # KubernetesPodOperator mode against k3d (needs `just k3d-up`)
just airflow-standalone   # scheduler and UI on :8080
```

Check that every DAG parses (no stack needed):

```bash
AIRFLOW_HOME=/tmp/af NEO4J_BACKUP_POLICY=policies/demo.yaml \
  airflow/.venv/bin/python -m pytest airflow/tests/
```
