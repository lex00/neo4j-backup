# Airflow

Airflow 3 DAGs run the policy's jobs (validated with Airflow 3.2.2). They share `neo4j_backup_core`
with the CLI, the MCP server and the [Dagster code location](../orchestrator/README.md).
Glossary: [group, tier, chain, full and diff](../GLOSSARY.md).

## The DAGs

DAG files in `airflow/dags/` import `orchestrator/neo4j_backup_airflow/`. Scheduled DAGs come
from the policy at parse time: one set per tier (empty tiers included).

| DAG | Schedule | Action |
|---|---|---|
| `neo4j_backup_<tier>_full`, `neo4j_backup_<tier>_diff` | `full_cron`, `diff_cron` | Back up the tier, or one group with Param `group_id`. Diff starts each member's first chain with a full. |
| `neo4j_verify_<tier>` | `verify_cron` (default daily 06:00) | Aggregate a copy of the newest chain and run `neo4j-admin database check` on it. |
| `neo4j_aggregate_<tier>` | `aggregate_cron` (default: unset) | Collapse each chain into one full in place. |
| `neo4j_prune_<tier>` | `prune_cron` (default Sundays 07:00) | Delete whole chains past `retention_days`. |
| `neo4j_cleanup_<tier>` | `cleanup_cron` | Drop superseded databases (keeps `cleanup_keep` and `cleanup_older_than_days`). |
| `neo4j_rebuild_<tier>` | `rebuild_cron` | Load each `load.mode: rebuild` group. |
| `neo4j_dbms_metadata_<cluster>`, `neo4j_dbms_system_<cluster>` | `dbms.metadata_cron`, `dbms.system_backup_cron` | Export security metadata as Cypher; back up `system`. |
| `neo4j_status` | `dbms.status_cron` (default every 15 minutes) | Fails when a target is past its `rpo_minutes`. |
| `neo4j_restore` | manual | Restore a group (Params below). |
| `neo4j_rollback` | manual | Point aliases back at the previous databases. Params: `group_id`, `members`, `ticket`, `reason`. |
| `neo4j_restore_to_cluster` | manual | Restore groups into another cluster. Params: `to_cluster`, `group_id` or `from_cluster`, `until`, `fence_source`, `skip_metadata`, `baseline`, `ticket`, `reason`. |
| `neo4j_load` | manual | Load or rebuild one group. Params: `group_id`, `replace`, `keep_failed`, `baseline`, `ticket`, `reason`. |
| `neo4j_metadata_restore` | manual | Replay a metadata export. Params: `cluster`, `key`. |
| `neo4j_backup_scaffold` | manual | Print the policy's targets. |

The DBMS DAGs exist per cluster a group uses (`default` when the policy omits `clusters:`).

`neo4j_restore` Params:

| Param | Default | |
|---|---|---|
| `group_id` | required | the group |
| `dry_run` | `false` | log the plan in the `plan` task and stop |
| `restore_until` | unset | ISO-8601 time (UTC by default). Unset: the newest instant all members cover. |
| `members` | `[]` | only these members |
| `check` | `false` | run the group's `load.checks` before the swap |
| `keep_failed` | `false` | with `check`: keep databases that fail a check |
| `replace` | `false` | by-name groups: drop and recreate an existing database |
| `from_load` | `false` | seed each member from its newest load |
| `from_dump` | `{}` | `{member: key or URI}` |
| `baseline` | `true` | take a full backup of each new database |
| `ticket`, `reason` | unset | recorded with the result |

Run it with `dry_run: true` first. `restore_until` is `until` on `neo4j_restore_to_cluster`.

## Deploying in production

The kit in [`airflow/deploy/`](deploy/) targets KubernetesExecutor (the official Helm chart):

- [`Dockerfile`](deploy/Dockerfile): Airflow plus the package and DAGs. Target `subprocess` adds `neo4j-admin`.
- [`pod-template.yaml`](deploy/pod-template.yaml): the task pods' environment and secrets.
- [`rbac.yaml`](deploy/rbac.yaml): the Role for starting `neo4j-admin` pods in `k8s` mode.

```bash
docker build -f airflow/deploy/Dockerfile --target k8s \
  --build-arg AIRFLOW_VERSION=3.2.2 --build-arg PYTHON_VERSION=3.12 \
  -t <registry>/airflow-neo4j-backup:0.7.0 .
```

For Azure add `azure-storage-blob` and `azure-identity` to the second `pip install`; for GCS,
`google-cloud-storage`. On another executor, install the same way and copy `airflow/dags/*.py`.

### Environment and policy

Set the [environment variables](../ENVIRONMENT.md) on the scheduler, DAG processor and task pods.
With the Helm chart use `env` and `extraEnvFrom`, and the pod template for task pods.
Set `NEO4J_BACKUP_POLICY` to an `s3://` path or a file mounted at the same path everywhere.
A policy that fails to load shows as an import error on every DAG file built from it.

### Pools

```bash
airflow pools set neo4j_full 1 "full backups, verifies, aggregates, loads"
airflow pools set neo4j_diff 6 "differential backups"
# per named cluster: neo4j_<cluster>_full and neo4j_<cluster>_diff
```

Verifies, aggregates and rebuilds use the full pool. Manual restore and load DAGs always use `neo4j_full`.

### Running neo4j-admin

With `RUNNER_MODE=subprocess` (default) neo4j-admin runs on the worker: use the `subprocess` target.
With `RUNNER_MODE=k8s` each command runs in a pod (`KubernetesPodOperator`). Set the `RUNNER_*`
variables and bind [`deploy/rbac.yaml`](deploy/rbac.yaml) to the worker's service account.
See [Choosing a runner mode](../ENVIRONMENT.md#choosing-a-runner-mode) and [Permissions](../ENVIRONMENT.md#permissions).

### Turn it on

1. Trigger `neo4j_backup_scaffold` to print the policy's targets.
2. Set `NEO4J_BACKUP_ALERT_WEBHOOK`. Each DAG posts failed tasks to it.
3. Try one group: trigger `neo4j_backup_<tier>_full` with `{"group_id": "<group>"}`, then
    `neo4j_verify_<tier>`, then `neo4j_restore` with `dry_run` ([Try a test group](../workflows/onboard.md#try-a-test-group)).

4. Unpause the scheduled DAGs you use and `neo4j_status`.
5. Unpause the manual DAGs (`neo4j_restore`, `neo4j_rollback`, `neo4j_load`, ...). Runs on a paused DAG wait.
6. Add an independent check that alerts when Airflow is down ([Alerts](../workflows/coverage.md#alerts)).

??? note "Use Airflow notifiers in place of the webhook"

    A task policy in `airflow_local_settings.py` replaces the webhook callback:

    ```python
    from airflow.providers.slack.notifications.slack import send_slack_notification

    def task_policy(task):
        if "neo4j-backup" in (task.dag.tags if task.dag else []):
            task.on_failure_callback = send_slack_notification(
                slack_conn_id="slack", text="{{ dag.dag_id }}.{{ ti.task_id }} failed")
    ```

### The CLI on Kubernetes

`restore-points`, `status`, `targets` and `policy show` run from the CLI only. Run it from a pod:

```bash
kubectl -n airflow run neo4j-backup-ops --rm -it --image=<registry>/airflow-neo4j-backup:0.7.0 \
  --overrides='{"spec":{"serviceAccountName":"airflow-worker","containers":[{"name":"neo4j-backup-ops","image":"<registry>/airflow-neo4j-backup:0.7.0","stdin":true,"tty":true,"command":["bash"],"envFrom":[{"secretRef":{"name":"neo4j-backup-env"}}],"env":[{"name":"NEO4J_BACKUP_POLICY","value":"s3://ops-config/neo4j-backup/policy.yaml"},{"name":"AWS_REGION","value":"eu-west-1"}]}]}}'
neo4j-backup --json restore-points shop
```

Commands that run `neo4j-admin` need the `subprocess` image or `RUNNER_MODE=k8s`.

## Run it locally

Against the Compose stack (`just fresh`):

```bash
just airflow-install      # its own venv (uv, Python 3.13) with pinned Airflow
just airflow-smoke        # backup, verify, restore, prune (dag.test, in-process)
just airflow-pitr         # a real full + diff chain and a point-in-time restore
just airflow-k8s-smoke    # KubernetesPodOperator mode against k3d (needs `just k3d-up`)
just airflow-standalone   # scheduler and UI on :8080
```

Check that every DAG parses (the stack can stay down):

```bash
AIRFLOW_HOME=/tmp/af NEO4J_BACKUP_POLICY=policies/demo.yaml \
  airflow/.venv/bin/python -m pytest airflow/tests/
```
