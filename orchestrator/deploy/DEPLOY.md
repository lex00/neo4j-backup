# Deploying on Dagster

How to run the `neo4j_backup_dagster` code location on a production Dagster instance (OSS on
Kubernetes with Helm, OSS on a VM, or Dagster+). What it defines is on the
[Dagster page](../README.md); every variable is on [ENVIRONMENT.md](../../ENVIRONMENT.md).

Requirements: Dagster 1.10 or later (the assets use `pool=`), Python 3.10 or later, and Neo4j
Enterprise servers prepared as in
[Prepare your Neo4j servers](../../GETTING-STARTED.md#prepare-your-neo4j-servers).

## 1. Build the image

[`Dockerfile`](Dockerfile) builds a code-location image on the Neo4j Enterprise image, which
supplies the JDK and `neo4j-admin` (use your cluster's version). It includes the k8s runner and
`dagster-postgres`, which run pods need on a Postgres-backed instance such as the Helm chart's.
Pin Dagster to your instance's version:

```bash
docker build -f orchestrator/deploy/Dockerfile \
  --build-arg NEO4J_VERSION=2026.05.0-enterprise --build-arg DAGSTER_VERSION=1.13.30 \
  -t <registry>/neo4j-backup-dagster:0.7.0 .
docker push <registry>/neo4j-backup-dagster:0.7.0
```

With `RUNNER_MODE=k8s` the code location doesn't run `neo4j-admin` itself, so a plain Python
image with `pip install "neo4j-backup-dagster[k8s,postgres] @ git+https://github.com/lex00/neo4j-backup@main#subdirectory=orchestrator"`
is enough (use a release tag in place of `main`), and `RUNNER_IMAGE` names the Neo4j image
the pods use. Which mode to choose:
[Choosing a runner mode](../../ENVIRONMENT.md#choosing-a-runner-mode).

## 2. Register the code location

### Helm

[`helm-values.yaml`](helm-values.yaml) is a working set of values for the `dagster` chart: the
code location with its image, service account, secret, environment, resources and scratch
volume, and the run limits from step 3. Create the secret it names (the file lists the keys),
merge the values into yours, and check the render:

```bash
kubectl create secret generic neo4j-backup-env --from-literal=NEO4J_PASSWORD=... # see the file
helm template dagster dagster/dagster -f your-values.yaml -f orchestrator/deploy/helm-values.yaml
helm upgrade --install dagster dagster/dagster -f your-values.yaml -f orchestrator/deploy/helm-values.yaml
```

`includeConfigInLaunchedRuns` gives every run pod the code location's environment, secrets and
volumes. The code reads its environment when the location loads (schedules come from the
policy) and again when a run starts, so both need it.

### Without Helm

OSS `workspace.yaml`, with the code server started by the image's default command:

```yaml
load_from:
  - grpc_server: { host: neo4j-backup, port: 4000, location_name: neo4j_backup }
```

Merge [`dagster.yaml`](dagster.yaml) into the instance's `dagster.yaml` for the run limits.

Dagster+ (`dagster_cloud.yaml`):

```yaml
locations:
  - location_name: neo4j_backup
    code_source: { module_name: neo4j_backup_dagster.definitions }
    image: <registry>/neo4j-backup-dagster:0.7.0
```

Check the location with the same environment:
`dagster definitions validate -m neo4j_backup_dagster.definitions`.

## 3. Concurrency

The run limits (in `helm-values.yaml` under `dagsterDaemon.runCoordinator`, or `dagster.yaml`
without Helm) add tag limits for this tool's tags only. Then set the op pool once, from any pod
with the instance's `dagster.yaml` (on Helm, the daemon):

```bash
kubectl exec deploy/dagster-daemon -- dagster instance concurrency set neo4j 4
```

(or Deployment, Concurrency in the UI). What the limits mean in practice:

- `backup_kind: full` (limit 1): fulls, verifies and aggregates run one at a time, because
  each needs scratch the size of a database. With N members whose fulls take D each, a full
  cycle takes about N × D; pick `full_cron` so it fits.
- `backup_kind: diff` (limit 6): up to six diffs at once.
- `neo4j_group` (limit 1): one run per group. A group's members are backed up one after
  another, and a full of one member delays the others' diffs by its length. With an hourly
  `diff_cron`, a 3-hour full means the group's other members can go about 3 hours without a
  diff on that night; set `rpo_minutes` accordingly, or put members with long fulls in groups
  of their own.
- `neo4j_cluster` (limit 4): at most four of this tool's runs against one Neo4j cluster.
- pool `neo4j`: the backup steps (and a restore's or load's baseline backup) across the code
  location. A restore itself doesn't wait for the full lane.

Runs launched by hand or as a backfill don't get the tags. The ops (and the CLI) also take a
per-group lock in the bucket (`_locks/<group>.lock`, the same one Airflow takes), so a member's
backup, verify, aggregate and its group's load never overlap whatever launched them; a run
waits up to `NEO4J_BACKUP_LOCK_WAIT` seconds for it.

## 4. Kubernetes runner mode

With `RUNNER_MODE=k8s` every neo4j-admin command is a pod created by the run (via
`PipesK8sClient`):

- The run pod's service account needs to create, watch and delete pods and read pod logs in
  `RUNNER_NAMESPACE` (default: the run pod's namespace). Ephemeral volumes need the
  `persistentvolumeclaims` create permission too. The Role in
  [`airflow/deploy/rbac.yaml`](../../airflow/deploy/rbac.yaml) lists them.
- Each pod gets a scratch volume of `RUNNER_SCRATCH_STORAGE` (default `6Ti`, for every pod,
  diffs included), a memory limit of `RUNNER_MEMORY_LIMIT` (default `4Gi`), and optional
  `RUNNER_CPU_REQUEST`, `RUNNER_MEMORY_REQUEST`, `RUNNER_TOLERATIONS`, `RUNNER_NODE_SELECTOR`.
- Credentials: give the pods a service account with IRSA or workload identity
  (`RUNNER_SERVICE_ACCOUNT`), or map each secret to a Kubernetes Secret with
  `RUNNER_SECRET_ENV`. The tool refuses to write a credential into a pod spec as plain text.
- `BACKUP_UPLOAD=pipeline` doesn't work in this mode and is refused at startup. A bucket that
  requires an encryption header on every PUT can't be written by neo4j-admin directly: use the
  bucket's default encryption (SSE-KMS with your key) or the subprocess mode.

What each component needs (bucket, KMS, secrets, ports) is in
[Permissions](../../ENVIRONMENT.md#permissions).

## 5. Encryption and per-group keys

S3 default encryption on the bucket covers neo4j-admin's writes. For a separate key or bucket
per group, define named stores in the policy (`storage:` with `sse_kms_key_id`, `write_args`,
`credentials_ref`) and set `storage:` on the groups; see [POLICY.md](../../POLICY.md#storage).
Grant the Neo4j servers read access and `kms:Decrypt` on every bucket and key a restore can
use: seeding is done by the servers.

## 6. Turn it on

1. Set `NEO4J_BACKUP_ALERT_WEBHOOK` in the secret. The `alert_on_failure` sensor then starts
   with the location and posts every failed run of it (a backup, `status_check` finding a
   stale database, a restore) to the webhook. Set `DAGSTER_BASE_URL` for links to the run.
2. Try one group: in the UI, open the `backup` asset, pick the group's partitions
   (`<group>/<alias>`) and materialize them with kind `FULL`; then `verify`; then launch
   `restore_group` with `dry_run: true` to see the plan, and without it on a test group (see
   [Try a test group](../../OPERATOR.md#try-a-test-group)).
3. Turn on the `reconcile_registry` sensor, the tier schedules and `status_check` (or set
   `schedules: running` in the policy, which starts all of them).
4. Add the independent check from [Alerts](../../OPERATOR.md#alerts), which alerts when Dagster
   itself is down.

The `backup_taken` check warns when a backup was skipped (no full yet to build on).

### Reloading

A new or changed cron, tier, cluster or `schedules:` value becomes a schedule when the location
loads, so reload it: Deployment, the location, Reload in the UI, or
`kubectl rollout restart deploy/<release>-dagster-user-deployments-neo4j-backup` on Helm.
Adding a group or member doesn't need one: the sensor picks it up within five minutes.

## The CLI on Kubernetes

The operator CLI (`neo4j-backup`) is in the same image. For a preview, `restore-points`,
`status`, `targets`, `policy show` or a one-off command, exec into the code location or start a
pod from the image with the same environment:

```bash
kubectl exec -it deploy/<release>-dagster-user-deployments-neo4j-backup -- \
  neo4j-backup --json restore-points shop
kubectl run neo4j-backup-ops --rm -it --image=<registry>/neo4j-backup-dagster:0.7.0 \
  --overrides='{"spec":{"serviceAccountName":"neo4j-backup"}}' \
  --env=NEO4J_BACKUP_POLICY=s3://ops-config/neo4j-backup/policy.yaml -- bash
```

In the second form also pass the secret's variables (or add `envFrom` to the overrides).
Commands that run `neo4j-admin` (`backup`, `verify`, `aggregate`, `system-backup`, `export`,
`load` with an import build) need it in the image or `RUNNER_MODE=k8s`; the rest (`status`,
`restore-points`, `restore`, `rollback`, `cleanup`, `adopt` without `--migrate`, `metadata`,
`prune`, `targets`, `policy`) only need Bolt and the bucket.

## Upgrading

Read the CHANGELOG's notes for each version. For 0.6.0 in particular:

- No `<tier>_aggregate` schedule unless the tier sets `aggregate_cron` (before, every backed-up
  tier aggregated weekly, which removed point-in-time restore inside the week).
- New: `<tier>_cleanup` (when `cleanup_cron` is set), `status_check`, the `neo4j_group` tag
  limit and the `neo4j` pool limit (step 3), and the `RUNNER_*` k8s settings.
- The sample `dagster.yaml` no longer sets `max_concurrent_runs` or a pool `default_limit`;
  remove them from your instance config if you copied them.
