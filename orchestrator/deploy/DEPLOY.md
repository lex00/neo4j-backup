# Deploying on Dagster

Run the code location on OSS Dagster (Helm or a VM) or Dagster+. See the [Dagster page](../README.md)
for what it defines and [ENVIRONMENT.md](../../ENVIRONMENT.md) for every variable.

Requirements: Dagster 1.10 or later and Python 3.10 or later.
Prepare the Neo4j servers as in [GETTING-STARTED.md](../../GETTING-STARTED.md#prepare-your-neo4j-servers).

## 1. Build the image

[`Dockerfile`](Dockerfile) builds on the Neo4j Enterprise image of your cluster's version, for `neo4j-admin`.
Pin Dagster to your instance's version:

```bash
docker build -f orchestrator/deploy/Dockerfile \
  --build-arg NEO4J_VERSION=2026.05.0-enterprise --build-arg DAGSTER_VERSION=1.13.30 \
  -t <registry>/neo4j-backup-dagster:0.7.0 .
docker push <registry>/neo4j-backup-dagster:0.7.0
```

With `RUNNER_MODE=k8s` a plain Python image is enough
([Choosing a runner mode](../../ENVIRONMENT.md#choosing-a-runner-mode)). Use a release tag in place of `main`:

```bash
pip install "neo4j-backup-dagster[k8s,postgres] @ git+https://github.com/lex00/neo4j-backup@main#subdirectory=orchestrator"
```

## 2. Register the code location

### Helm

Create the secret that [`helm-values.yaml`](helm-values.yaml) names (it lists the keys), then merge
the values into yours:

```bash
kubectl create secret generic neo4j-backup-env --from-literal=NEO4J_PASSWORD=... # see the file
helm template dagster dagster/dagster -f your-values.yaml -f orchestrator/deploy/helm-values.yaml
helm upgrade --install dagster dagster/dagster -f your-values.yaml -f orchestrator/deploy/helm-values.yaml
```

Keep `includeConfigInLaunchedRuns`, which gives run pods the code server's environment and volumes.

### Without Helm

OSS `workspace.yaml` (the image's default command starts the code server):

```yaml
load_from:
  - grpc_server: { host: neo4j-backup, port: 4000, location_name: neo4j_backup }
```

Dagster+ (`dagster_cloud.yaml`):

```yaml
locations:
  - location_name: neo4j_backup
    code_source: { module_name: neo4j_backup_dagster.definitions }
    image: <registry>/neo4j-backup-dagster:0.7.0
```

Without Helm, merge [`dagster.yaml`](dagster.yaml) into the instance's for the run limits. Check the
location with `dagster definitions validate -m neo4j_backup_dagster.definitions`.

## 3. Concurrency

The run limits come from step 2. Set the op pool once, from a pod with the instance's `dagster.yaml`:

```bash
kubectl exec deploy/dagster-daemon -- dagster instance concurrency set neo4j 4
```

| Limit | Value | Effect |
|---|---|---|
| tag `backup_kind: full` | 1 | Fulls, verifies and aggregates run one at a time. |
| tag `backup_kind: diff` | 6 | Diffs at once. |
| tag `neo4j_group` | 1 | One run per group: a full delays the group's diffs. |
| tag `neo4j_cluster` | 4 | Runs per Neo4j cluster. |
| pool `neo4j` | 4 | Backup steps across the location. |

N fulls of D hours take about N x D, so pick `full_cron` to fit. Allow for a long full in `rpo_minutes`,
or give that member its own group. Manual runs skip the tags, but the group lock still applies.

## 4. Kubernetes runner mode

With `RUNNER_MODE=k8s` each neo4j-admin command is a pod (`PipesK8sClient`):

1. In `RUNNER_NAMESPACE`, grant the run pod's service account the Role in
    [`rbac.yaml`](../../airflow/deploy/rbac.yaml) (pods, pod logs, `persistentvolumeclaims`).

2. Size the pods with the `RUNNER_*` variables.
3. Give the pods credentials with `RUNNER_SERVICE_ACCOUNT` (IRSA, workload identity) or `RUNNER_SECRET_ENV`.

`BACKUP_UPLOAD=pipeline` is refused here, so rely on bucket default encryption.
See [Permissions](../../ENVIRONMENT.md#permissions).

## 5. Encryption and per-group keys

For a key or bucket per group, define named stores in the policy and set `storage:` on the groups.
A store takes `sse_kms_key_id`, `write_args` and `credentials_ref` ([POLICY.md](../../POLICY.md#storage)).
The Neo4j servers need read access and `kms:Decrypt` on each of them.

## 6. Turn it on

1. Set `NEO4J_BACKUP_ALERT_WEBHOOK` (and `DAGSTER_BASE_URL`) to get failed runs posted.
2. Try one group: materialize the `backup` asset for its partitions (`<group>/<alias>`) with
    kind `FULL`, then `verify`. Launch `restore_group` with `dry_run: true`, then without it on a test
    group ([Try a test group](../../workflows/onboard.md#try-a-test-group)).

3. Turn on `reconcile_registry`, the tier schedules and `status_check`, or set `schedules: running`
    in the policy.

4. Add an independent check that alerts when Dagster is down ([Alerts](../../workflows/coverage.md#alerts)).

### Reloading

Reload the location after cron, tier, cluster or `schedules:` changes. New groups need no reload.
Use Reload in the UI, or on Helm:

```bash
kubectl rollout restart deploy/<release>-dagster-user-deployments-neo4j-backup
```

## The CLI on Kubernetes

The `neo4j-backup` CLI is in the same image:

```bash
kubectl exec -it deploy/<release>-dagster-user-deployments-neo4j-backup -- \
  neo4j-backup --json restore-points shop
kubectl run neo4j-backup-ops --rm -it --image=<registry>/neo4j-backup-dagster:0.7.0 \
  --overrides='{"spec":{"serviceAccountName":"neo4j-backup"}}' \
  --env=NEO4J_BACKUP_POLICY=s3://ops-config/neo4j-backup/policy.yaml -- bash
```

In the second form also pass the secret's variables (or add `envFrom` to the overrides).

## Upgrading

Read the [CHANGELOG](../../CHANGELOG.md) for each version. From before 0.6.0, set `aggregate_cron`
to keep a tier's aggregate schedule. Add the `neo4j_group` limit and the `neo4j` pool. Remove
`max_concurrent_runs` and a pool `default_limit` from `dagster.yaml`.
