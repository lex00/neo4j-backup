# Environment variables

Every variable the CLI, the MCP server, the Dagster code location and the Airflow DAGs read.
The same names apply to all of them. Neither adapter uses Airflow Connections, Airflow
Variables or Dagster resource config for these: set them in the process environment.

Where they must be set:

- Dagster: on the code-location server and on every run worker or run pod (they're read when
  the code location loads, and again when a run starts; with Helm, see
  [deploy/DEPLOY.md](orchestrator/deploy/DEPLOY.md)).
- Airflow: on the scheduler, the DAG processor and every worker. DAG files are generated from
  the policy when they're parsed, so the DAG processor needs the policy too.
- CLI and MCP: in the shell or service that runs them.

Settings that differ per cluster or per store (Bolt URI, backup source, bucket, credentials)
can instead go in the policy's `clusters:` and `storage:` blocks
([POLICY.md](POLICY.md#policy-or-environment)). The variables below then describe the
`default` cluster and store.

## Policy

| Variable | Default | What it does |
|---|---|---|
| `NEO4J_BACKUP_POLICY` | `policies/demo.yaml` | Path or `s3://bucket/key.yaml` of the policy. The default is relative to the process's working directory, so always set it. |
| `POLICY_CACHE_TTL` | `60` | Seconds a loaded policy is reused (`0`: read every time). A failed re-read keeps the last good policy. Schedules and DAGs are built when the code location or DAG file loads, so cron, tier and cluster changes need a reload; group membership is read live. |
| `POLICY_LOADER` | unset | `module.callable` (`(source) -> str`) that fetches the policy yourself (an authenticated endpoint, Vault). |

## Neo4j (the default cluster)

| Variable | Default | What it does |
|---|---|---|
| `NEO4J_BOLT_URI` | `neo4j://localhost:7687` | Bolt address for Cypher (restore, aliases, privileges, checks). |
| `NEO4J_USER` | `neo4j` | User for Bolt. Needs admin rights on `system` (CREATE/ALTER/DROP DATABASE and ALIAS, SHOW PRIVILEGES, GRANT). |
| `NEO4J_PASSWORD` | none | The password, when `SECRET_PROVIDER=env` (the default). |
| `SECRET_PROVIDER` | `env` | `env` reads `NEO4J_PASSWORD`; `aws-sm` reads the secret named by `NEO4J_PASSWORD_REF` from AWS Secrets Manager. |
| `NEO4J_PASSWORD_REF` | unset | The secret id or ARN for `aws-sm` (optionally `#json_key`). |
| `NEO4J_BACKUP_SOURCE` | `neo4j:6362` | Backup address(es) for `neo4j-admin database backup --from` (a secondary's backup port on a cluster; comma-separate several). |
| `NEO4J_RETRY_ATTEMPTS` / `NEO4J_RETRY_BASE` / `NEO4J_RETRY_CAP` | `5` / `0.2` / `5.0` | Retries for transient Bolt errors (leader change, dropped session, expired token). |
| `NEO4J_DRIVER_MAX_AGE` | `600` | Seconds a client reuses its Bolt driver before reconnecting (and re-reading the secret). Any error also reconnects. |
| `NEO4J_SEED_WAIT_SECONDS` | `3600` | How long a restore waits for `CREATE DATABASE … WAIT` when the group sets no `rto_minutes`. A seed that hasn't finished in time fails before any alias moves. |
| `SEED_CYPHER_VERSION` | unset | `5` or `25`: the Cypher version the seed statement uses (`5` adds `existingData: 'use'`). Unset: the server default. |

## Object store (the default store)

| Variable | Default | What it does |
|---|---|---|
| `CLOUD` | `aws` | `aws` (S3), `azure` (Blob) or `gcp` (GCS). Azure and GCS need the `[azure]` / `[gcp]` extras. |
| `BACKUP_BUCKET` | `neo4j-backups` | Bucket, or Azure container. |
| `AWS_REGION` | `us-east-1` | Region. |
| `AWS_ENDPOINT_URL_S3` | unset | Only for S3-compatible stores and emulators; leave unset on AWS. |
| `AZURE_STORAGE_CONNECTION_STRING` | none | Azure credentials (`CLOUD=azure`). |
| `AZURE_STORAGE_ACCOUNT` | `devstoreaccount1` | Account name used in the `azb://` URIs Neo4j seeds from. |
| `AZURE_COPY_TIMEOUT` | `3600` | Seconds to wait for one server-side copy (verify's scratch copy) to finish. |
| `STORAGE_EMULATOR_HOST` | unset | fake-gcs-server address (`CLOUD=gcp`, tests only). GCS otherwise uses application default credentials. |
| `S3_SSE` / `S3_SSE_KMS_KEY_ID` | unset | Encryption header on the tool's own S3 writes (metadata exports, sidecars, verify copies, and with `BACKUP_UPLOAD=pipeline` the backups). |
| `S3_WRITE_ARGS` | `{}` | JSON of extra arguments for those writes. Per cloud: see `write_args` in [POLICY.md](POLICY.md#storage). |
| `BACKUP_UPLOAD` | `admin` | `admin`: neo4j-admin writes backups straight to the bucket (no encryption header; the bucket's default encryption applies). `pipeline`: neo4j-admin writes to local disk and the tool uploads with `S3_SSE`, for buckets that refuse PUTs without the header. `pipeline` works only with `RUNNER_MODE=subprocess` and is refused with `k8s`. |
| `UPLOAD_STAGING_PATH` | `SCRATCH_PATH` | Local directory for `pipeline` mode. |

AWS credentials come from the usual chain (instance role, IRSA, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).
The Neo4j servers fetch seeds with their own cloud settings, so they need read access (and
`kms:Decrypt`) on every bucket a restore uses.

## Runner (where neo4j-admin runs)

| Variable | Default | What it does |
|---|---|---|
| `RUNNER_MODE` | `subprocess` | `subprocess`: neo4j-admin runs on the worker (or CLI host). `k8s`: each command runs in its own pod. |
| `RUNNER_NEO4J_ADMIN` | `neo4j-admin` | Path to the binary (subprocess mode). |
| `RUNNER_EXEC_PREFIX` | `[]` | JSON argv put in front of every command, e.g. `["docker","exec","-T","runner"]` to run neo4j-admin in a container (all front-ends). |
| `SCRATCH_PATH` | `/scratch` | Scratch directory: room for the largest full backup. |
| `RUNNER_PAGECACHE` | `512M` | neo4j-admin `--pagecache`. |
| `RUNNER_HEAP_SIZE` | `2G` | neo4j-admin heap (passed as `HEAP_SIZE`). |
| `RUNNER_IMAGE` | none | k8s: the image for neo4j-admin pods (a Neo4j Enterprise image of the cluster's version). |
| `RUNNER_NAMESPACE` | Airflow: `default`; Dagster: the run pod's | k8s: namespace for the pods. |
| `RUNNER_IN_CLUSTER` | `true` | Airflow k8s: use the in-cluster Kubernetes config. |
| `RUNNER_SERVICE_ACCOUNT` | none | k8s: the pods' service account (for IRSA or workload identity). |
| `RUNNER_MEMORY_LIMIT` | `4Gi` | k8s: memory limit per pod. Keep above heap + page cache. |
| `RUNNER_MEMORY_REQUEST` / `RUNNER_CPU_REQUEST` | unset | k8s: resource requests per pod. |
| `RUNNER_SCRATCH_STORAGE` | `6Ti` | k8s: size of each pod's ephemeral scratch volume. It applies to every pod, diffs included; set it to what your largest full needs. |
| `RUNNER_NODE_SELECTOR` | `{}` | k8s: JSON node selector. |
| `RUNNER_TOLERATIONS` | `[]` | k8s: JSON list of tolerations. |
| `RUNNER_SECRET_ENV` | `{}` | k8s: JSON `{"ENV_NAME": "<secret>:<key>"}`. Credentials the tool passes to a pod (a named store's keys, a build's `secret:` env) come from these Secrets; a credential with no entry here is refused rather than written into the pod spec. |
| `RUNNER_EXTRA_ENV` | `{}` | JSON env added to every pod as plain values (visible to anyone who can read pods). |

## Choosing a runner mode

Subprocess mode is the simpler start: `neo4j-admin` runs where the job runs, so the Dagster image
(`orchestrator/deploy/Dockerfile`) or the Airflow worker image (`airflow/deploy/Dockerfile
--target subprocess`) carries it. Every run that backs up needs scratch space as large as its
backup; on Kubernetes that means one ReadWriteMany volume (EFS, Filestore, Azure Files) mounted
in every run pod, sized for the fulls that can run at once (the full lane's limit times your
largest full).

k8s mode starts a pod from `RUNNER_IMAGE` for each `neo4j-admin` command, with its own
ephemeral scratch volume (`RUNNER_SCRATCH_STORAGE`), so nothing is shared and the worker image
stays small. It needs RBAC to create pods (`airflow/deploy/rbac.yaml`; Dagster's run pods need
the same in their namespace) and a storage class that provisions ephemeral volumes. It can't be
combined with `BACKUP_UPLOAD=pipeline`.

Memory, both modes: `RUNNER_HEAP_SIZE` plus `RUNNER_PAGECACHE` plus about 1 GiB for the JVM and
Python. In subprocess mode that is the run pod's (or worker's) limit; in k8s mode
`RUNNER_MEMORY_LIMIT`.

## Permissions

| Who | Needs |
|---|---|
| Dagster code server, Airflow scheduler and DAG processor | read the policy (file or `s3://` object); the secrets in the environment |
| Dagster run pods, Airflow workers or task pods | the bucket: list, read, write, delete (prune, verify scratch, locks); `kms:GenerateDataKey` and `kms:Decrypt` on an encrypted bucket; Secrets Manager read with `SECRET_PROVIDER`; Bolt (7687) to every server; in subprocess mode, the backup port (6362) |
| k8s runner pods (`RUNNER_SERVICE_ACCOUNT`) | the bucket and KMS as above; the backup port (6362) |
| Neo4j servers | the bucket: read and list; `kms:Decrypt` (they fetch seeds themselves) |

## Behaviour

| Variable | Default | What it does |
|---|---|---|
| `NEO4J_BACKUP_REQUIRE_TICKET` | unset | `true`: every applied change needs `--ticket` (CLI), `ticket` (MCP, Dagster config, Airflow params). |
| `NEO4J_BACKUP_LOCK_WAIT` | `3600` | Seconds a backup, verify, aggregate or load (from any front-end, the CLI included) waits for its group's lock before failing with `locked`. The lock, `_locks/<group>.lock` in the default store, expires 6 hours after it was taken, so a crashed run holds it at most that long; delete the object to clear it sooner. |
| `NEO4J_BACKUP_ALERT_WEBHOOK` | unset | A URL that failed runs are posted to as JSON `{"text": …}` (Slack, Teams, Mattermost and most chat webhooks accept it). Dagster: the `alert_on_failure` sensor, on when this is set. Airflow: every DAG's failure callback. |
| `DAGSTER_BASE_URL` | unset | Dagster: the UI's URL, so alerts link to the failed run. |
| `NEO4J_BACKUP_MCP_MODE` | `read-only` | MCP server: `read-write` enables the guarded write tools. |
| `NEO4J_BACKUP_DEBUG` | unset | CLI: print the traceback of an unexpected error to stderr. |
| `CUTOVER_STRATEGY` / `CUTOVER_HOOK` | `alias-swap` / none | `external` calls `CUTOVER_HOOK` (an http(s) URL or a command) instead of `ALTER ALIAS`. |
| `PATH_LAYOUT` | unset | `module.Class`: your own bucket key layout. |
| `NAMING` | unset | `module.Class`: your own database naming scheme. |
| `NAMING_TS_FMT` | `%Y%m%dt%H%M%S` | strftime for the timestamp in new database names (no `-`). |
