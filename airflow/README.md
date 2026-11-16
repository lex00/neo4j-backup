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
| `neo4j_backup_<tier>_full`, `neo4j_backup_<tier>_diff` | the tier's `full_cron`, `diff_cron` | Back up every member of the tier, or one group with Param `group_id`. The diff DAG takes a full instead for members with no full to build on yet (just restored or new). |
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
`neo4j_diff`); verifies, aggregates and rebuilds use the cluster's full pool, since each needs
scratch the size of a full. The DBMS DAGs exist for each cluster a group uses (`default` only
when a group uses it or the policy has no `clusters:`). The manual restore and load
DAGs always use `neo4j_full` for their baseline and load tasks: the group is a run parameter
and Airflow fixes a task's pool when it parses the DAG.

`neo4j_restore` Params:

| Param | Default | |
|---|---|---|
| `group_id` | required | the group |
| `dry_run` | `false` | log the plan (restore point, seeds, privileges to copy, anything dropped) and stop; nothing changes |
| `restore_until` | none | ISO-8601 time (UTC if no offset); without it the group goes back to the newest instant every member covers |
| `members` | `[]` | only these members |
| `check` | `false` | run the group's `load.checks` on the new databases before the swap |
| `keep_failed` | `false` | with `check`: keep the new databases when a check fails |
| `replace` | `false` | by-name groups: drop and recreate an existing database |
| `from_load` | `false` | seed each member from its newest load |
| `from_dump` | `{}` | `{member: key or URI}` |
| `baseline` | `true` | take a full backup of each new database afterwards |
| `ticket`, `reason` | none | recorded in the log and the result |

Trigger it with `dry_run: true` first and read the `plan` task's log; then trigger it again
without. The Param is `restore_until` here and `until` on `neo4j_restore_to_cluster` (and
`--until` in the CLI); they take the same values.

## Deploying in production

### The deploy kit

[`airflow/deploy/`](deploy/) has what a KubernetesExecutor deployment (the official Helm chart)
needs:

- [`Dockerfile`](deploy/Dockerfile): your Airflow image plus the package and the DAGs. Build it
  with your Airflow and Python versions (the constraints follow them). Target `k8s` for
  `RUNNER_MODE=k8s`; target `subprocess` adds the JDK and `neo4j-admin` from the Neo4j image.

  ```bash
  docker build -f airflow/deploy/Dockerfile --target k8s \
    --build-arg AIRFLOW_VERSION=3.2.2 --build-arg PYTHON_VERSION=3.12 \
    -t <registry>/airflow-neo4j-backup:0.7.0 .
  ```

  For CLOUD=azure or gcp add `azure-storage-blob` and `azure-identity`, or
  `google-cloud-storage`, to the second `pip install`.
- [`pod-template.yaml`](deploy/pod-template.yaml): the task pods' environment, with the
  secrets as `envFrom` and the `kubectl create secret` commands. Every task pod parses its DAG
  file, so it needs the same environment as the scheduler and DAG processor.
- [`rbac.yaml`](deploy/rbac.yaml): the Role and RoleBinding the task pods need to start
  `neo4j-admin` pods in `RUNNER_MODE=k8s`.

On another executor, install the same way as the Dockerfile and copy `airflow/dags/*.py` into
the DAGs folder (they import only `neo4j_backup_airflow` and `neo4j_backup_core`).

### Environment and policy

Everything is environment variables ([ENVIRONMENT.md](../ENVIRONMENT.md)); Airflow
Connections and Variables aren't used. Set them on the scheduler, the DAG processor and every
worker or task pod (with the Helm chart: `env` and `extraEnvFrom` for the scheduler and DAG
processor, the pod template for task pods). With several clusters, each cluster's password goes
in its own variable named by `password_ref: env:NEO4J_PASSWORD_<CLUSTER>`; `NEO4J_PASSWORD` is
used for clusters without one. The DAG processor builds the DAGs from the policy, so it must read
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

Each task also takes a per-group lock in the bucket (`_locks/<group>.lock`, shared with the
CLI and Dagster), so a member's backup, verify, aggregate and its group's rebuild never overlap;
a task waits up to `NEO4J_BACKUP_LOCK_WAIT` seconds for it.

### Running neo4j-admin

- `RUNNER_MODE=subprocess` (default): neo4j-admin runs on the worker. The worker image then
  needs a JDK and the Neo4j Enterprise distribution of your cluster's version, a scratch
  volume at `SCRATCH_PATH` big enough for the largest full, and network to the backup port
  (6362) and the bucket.
- `RUNNER_MODE=k8s`: each command runs in its own pod (`KubernetesPodOperator`, started from
  inside the task). Set `RUNNER_IMAGE` (the Neo4j Enterprise image), `RUNNER_NAMESPACE`
  (default `default`), and the sizes: `RUNNER_SCRATCH_STORAGE` (default `6Ti` per pod),
  `RUNNER_MEMORY_LIMIT` (default `4Gi`), `RUNNER_CPU_REQUEST`, `RUNNER_MEMORY_REQUEST`,
  `RUNNER_TOLERATIONS`, `RUNNER_NODE_SELECTOR`. The worker's service account needs the Role in
  [`deploy/rbac.yaml`](deploy/rbac.yaml) in that namespace.
  The pod image needs `sh`, `mkdir`, `printf` and `base64` (the Neo4j image has them). Give
  the pods cloud credentials through IRSA or workload identity (`RUNNER_SERVICE_ACCOUNT`), or
  map secrets with `RUNNER_SECRET_ENV`; credentials are never written into a pod spec as plain
  text. `BACKUP_UPLOAD=pipeline` is refused in this mode.

Restores, aliases and privileges are Cypher over Bolt from the worker. Which mode to choose,
and what each component needs: [Choosing a runner mode](../ENVIRONMENT.md#choosing-a-runner-mode)
and [Permissions](../ENVIRONMENT.md#permissions).

### Turn it on

1. Check the deployment: trigger `neo4j_backup_scaffold`; it prints the policy's targets.
2. Set `NEO4J_BACKUP_ALERT_WEBHOOK`. Every DAG here has a failure callback that posts a failed
   task to it (a backup, `neo4j_status` finding a stale database, a restore). `neo4j_status`
   runs every 15 minutes, so a database that stays stale alerts every 15 minutes until fixed.
   To use Airflow's own notifiers instead, attach them to the tasks of the `neo4j-backup`-tagged
   DAGs with a task policy in `airflow_local_settings.py` (it replaces the webhook callback):

   ```python
   from airflow.providers.slack.notifications.slack import send_slack_notification

   def task_policy(task):
       if "neo4j-backup" in (task.dag.tags if task.dag else []):
           task.on_failure_callback = send_slack_notification(
               slack_conn_id="slack", text="{{ dag.dag_id }}.{{ ti.task_id }} failed")
   ```
3. Try one group: trigger `neo4j_backup_<tier>_full` with `{"group_id": "<group>"}`, then
   `neo4j_verify_<tier>`, then `neo4j_restore` with `dry_run` (see
   [Try a test group](../OPERATOR.md#try-a-test-group)).
4. Unpause the scheduled DAGs you use and `neo4j_status`.
5. Unpause the manual DAGs too (`neo4j_restore`, `neo4j_rollback`, `neo4j_load`, ...): a
   triggered run of a paused DAG waits until it's unpaused.
6. Add the independent check from [Alerts](../OPERATOR.md#alerts), which alerts when Airflow
   itself is down.

### The CLI on Kubernetes

`restore-points`, `status`, `targets` and `policy show` have no DAG; run them with the CLI,
which is in the worker image. Under KubernetesExecutor there is no long-lived worker, so start
a pod from the image with the task pods' environment:

```bash
kubectl -n airflow run neo4j-backup-ops --rm -it --image=<registry>/airflow-neo4j-backup:0.7.0 \
  --overrides='{"spec":{"serviceAccountName":"airflow-worker","containers":[{"name":"neo4j-backup-ops","image":"<registry>/airflow-neo4j-backup:0.7.0","stdin":true,"tty":true,"command":["bash"],"envFrom":[{"secretRef":{"name":"neo4j-backup-env"}}],"env":[{"name":"NEO4J_BACKUP_POLICY","value":"s3://ops-config/neo4j-backup/policy.yaml"},{"name":"AWS_REGION","value":"eu-west-1"}]}]}}'
neo4j-backup --json restore-points shop
```

Commands that run `neo4j-admin` (`backup`, `verify`, `aggregate`, `system-backup`, `export`)
need the `subprocess` image target or `RUNNER_MODE=k8s` with its variables; the others only
need Bolt and the bucket. See [OPERATOR.md](../OPERATOR.md).

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
