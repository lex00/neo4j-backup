# Deploying on Dagster

How to run the `neo4j_backup_dagster` code location on a production Dagster instance (OSS on
Kubernetes, OSS on a VM, or Dagster+). What it defines is on the
[Dagster page](../README.md); every variable is on [ENVIRONMENT.md](../../ENVIRONMENT.md).

Requirements: Dagster 1.10 or later (the assets use `pool=`), Python 3.10 or later, and a
Neo4j Enterprise cluster. The tool needs a Neo4j user with admin rights on `system`.

## 1. Build the image

[`Dockerfile`](Dockerfile) builds a code-location image on the Neo4j Enterprise image, which
supplies the JDK and `neo4j-admin` (use the same version as your cluster):

```bash
docker build -f orchestrator/deploy/Dockerfile \
  --build-arg NEO4J_VERSION=2026.05.0-enterprise -t <registry>/neo4j-backup-dagster:0.6.0 .
```

With `RUNNER_MODE=k8s` the code location doesn't run `neo4j-admin` itself: a plain Python
image with `pip install "neo4j-backup-dagster[k8s] @ git+https://github.com/lex00/neo4j-backup@v0.6.0#subdirectory=orchestrator"`
is enough, and `RUNNER_IMAGE` names the Neo4j image the pods use.

## 2. Register the code location

OSS `workspace.yaml`:

```yaml
load_from:
  - grpc_server: { host: neo4j-backup, port: 4000, location_name: neo4j_backup }
```

Helm (`dagster-user-deployments`):

```yaml
dagster-user-deployments:
  deployments:
    - name: neo4j-backup
      image: { repository: <registry>/neo4j-backup-dagster, tag: "0.6.0" }
      dagsterApiGrpcArgs: ["-m", "neo4j_backup_dagster.definitions"]
      port: 4000
      includeConfigInLaunchedRuns: { enabled: true }   # run pods get the env below
      envSecrets: [ { name: neo4j-backup-env } ]       # NEO4J_PASSWORD, NEO4J_BOLT_URI, ...
      env:
        - { name: NEO4J_BACKUP_POLICY, value: "s3://ops-config/neo4j-backup/policy.yaml" }
      volumes:      [ { name: scratch, persistentVolumeClaim: { claimName: neo4j-scratch } } ]
      volumeMounts: [ { name: scratch, mountPath: /scratch } ]
```

Dagster+ (`dagster_cloud.yaml`):

```yaml
locations:
  - location_name: neo4j_backup
    code_source: { module_name: neo4j_backup_dagster.definitions }
    image: <registry>/neo4j-backup-dagster:0.6.0
```

The code reads its environment both when the location loads (schedules come from the
policy) and when a run starts, so the code server and every run worker or run pod need the
same variables and secrets. `includeConfigInLaunchedRuns` does that on Helm.

Then check it: `dagster definitions validate -m neo4j_backup_dagster.definitions` with the
same environment.

## 3. Concurrency

Merge [`dagster.yaml`](dagster.yaml) into the instance's `dagster.yaml` and set the op pool:

```bash
dagster instance concurrency set neo4j 4
```

The sample adds tag limits only for this tool's tags and sets nothing instance-wide.
What the limits mean in practice:

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

Runs launched by hand or as a backfill don't get the tags. The ops also take a per-group
lock in the bucket (`_locks/<group>.lock`, the same one Airflow takes), so a member's backup,
verify, aggregate and its group's load never overlap whatever launched them; a run waits up
to `NEO4J_BACKUP_LOCK_WAIT` seconds for it.

## 4. Kubernetes runner mode

With `RUNNER_MODE=k8s` every neo4j-admin command is a pod created by the run (via
`PipesK8sClient`):

- The run pod's service account needs to create, watch and delete pods and read pod logs in
  `RUNNER_NAMESPACE` (default: the run pod's namespace). Ephemeral volumes need the
  `persistentvolumeclaims` create permission too.
- Each pod gets a scratch volume of `RUNNER_SCRATCH_STORAGE` (default `6Ti`, for every pod,
  diffs included), a memory limit of `RUNNER_MEMORY_LIMIT` (default `4Gi`), and optional
  `RUNNER_CPU_REQUEST`, `RUNNER_MEMORY_REQUEST`, `RUNNER_TOLERATIONS`, `RUNNER_NODE_SELECTOR`.
- Credentials: give the pods a service account with IRSA or workload identity
  (`RUNNER_SERVICE_ACCOUNT`), or map each secret to a Kubernetes Secret with
  `RUNNER_SECRET_ENV`. The tool refuses to write a credential into a pod spec as plain text.
- `BACKUP_UPLOAD=pipeline` doesn't work in this mode and is refused at startup. A bucket that
  requires an encryption header on every PUT can't be written by neo4j-admin directly: use the
  bucket's default encryption (SSE-KMS with your key) or the subprocess mode.

## 5. Encryption and per-group keys

S3 default encryption on the bucket covers neo4j-admin's writes. For a separate key or bucket
per group, define named stores in the policy (`storage:` with `sse_kms_key_id`, `write_args`,
`credentials_ref`) and set `storage:` on the groups; see [POLICY.md](../../POLICY.md#storage).
Grant the Neo4j servers read access and `kms:Decrypt` on every bucket and key a restore can
use: seeding is done by the servers.

## 6. Turn it on

1. Turn on the `reconcile_registry` sensor (or set `schedules: running` in the policy, which
   starts the schedules and the sensor).
2. Materialize `backup` for one member, then `verify`, then run `restore_group` for a test
   group.
3. Turn on the tier schedules and `status_check`, and alert on failed runs (a run failure
   sensor or Dagster+ alerts). `status_check` fails when any target is past its
   `rpo_minutes`; the `backup_taken` check warns when a backup was skipped.

What needs a reload of the code location: a new or changed cron, tier, cluster or
`schedules:` value (they become schedules when the location loads). Adding a group or member
doesn't: the sensor picks it up within five minutes.

## Upgrading

Read the CHANGELOG's notes for each version. For 0.6.0 in particular:

- No `<tier>_aggregate` schedule unless the tier sets `aggregate_cron` (before, every backed-up
  tier aggregated weekly, which removed point-in-time restore inside the week).
- New: `<tier>_cleanup` (when `cleanup_cron` is set), `status_check`, the `neo4j_group` tag
  limit and the `neo4j` pool limit (step 3), and the `RUNNER_*` k8s settings.
- The sample `dagster.yaml` no longer sets `max_concurrent_runs` or a pool `default_limit`;
  remove them from your instance config if you copied them.
