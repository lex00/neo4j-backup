# Environment variables

Every variable the CLI, the MCP server, the Dagster code location and the Airflow DAGs read.
Terms (seed, runner, full): [GLOSSARY.md](GLOSSARY.md).
Dagster needs them on the code server and every run worker or pod.
Airflow needs them on the scheduler, the DAG processor and every worker or task pod.

Per-cluster and per-store settings can go in the policy's `clusters:` and `storage:` instead
([POLICY.md](POLICY.md#policy-or-environment)). The variables then describe the `default` cluster
and store. A named cluster's password goes in the variable its `password_ref` names, such as
`env:NEO4J_PASSWORD_<CLUSTER>`.

## Policy

| Variable | Default | Effect |
|---|---|---|
| `NEO4J_BACKUP_POLICY` | `policies/demo.yaml` | Path or `s3://bucket/key.yaml`. The default is relative to the working directory: always set it. |
| `POLICY_CACHE_TTL` | `60` | Seconds a loaded policy is reused (`0`: re-read every time). A failed re-read keeps the last good policy. |
| `POLICY_LOADER` | unset | `module.callable` (`(source) -> str`) that fetches the policy (Vault, an authenticated endpoint). |

Cron, tier and cluster changes need a reload of the code location or DAG files. Group membership is read live.

## Neo4j (the default cluster)

| Variable | Default | Effect |
|---|---|---|
| `NEO4J_BOLT_URI` | `neo4j://localhost:7687` | Bolt address for Cypher. |
| `NEO4J_USER` | `neo4j` | Bolt user, with admin rights on `system` (databases, aliases, privileges). |
| `NEO4J_PASSWORD` | unset | The password, with `SECRET_PROVIDER=env`. |
| `SECRET_PROVIDER` | `env` | `env` or `aws-sm` (AWS Secrets Manager). |
| `NEO4J_PASSWORD_REF` | unset | Secret id or ARN for `aws-sm` (optionally `#json_key`). |
| `NEO4J_BACKUP_SOURCE` | `neo4j:6362` | Backup address(es) for `--from`, comma-separated. Use a secondary on a cluster. |
| `NEO4J_SEED_WAIT_SECONDS` | `3600` | How long a restore waits for the seed when the group leaves `rto_minutes` unset. |
| `SEED_CYPHER_VERSION` | unset | `5` or `25`: Cypher version of the seed statement. Unset: server default. |

??? note "Bolt retries and reconnects"

    | Variable | Default | Effect |
    |---|---|---|
    | `NEO4J_RETRY_ATTEMPTS` / `NEO4J_RETRY_BASE` / `NEO4J_RETRY_CAP` | `5` / `0.2` / `5.0` | Retries for transient Bolt errors. |
    | `NEO4J_DRIVER_MAX_AGE` | `600` | Seconds before a driver reconnects and re-reads the secret. |

## Object store (the default store)

| Variable | Default | Effect |
|---|---|---|
| `CLOUD` | `aws` | `aws`, `azure` or `gcp`. Azure and GCS need the `[azure]` / `[gcp]` extras. |
| `BACKUP_BUCKET` | `neo4j-backups` | Bucket, or Azure container. |
| `AWS_REGION` | `us-east-1` | Region. |
| `AWS_ENDPOINT_URL_S3` | unset | S3-compatible stores and emulators only. |
| `S3_SSE` / `S3_SSE_KMS_KEY_ID` | unset | Encryption header on the tool's own S3 writes. |
| `S3_WRITE_ARGS` | `{}` | JSON of extra arguments for those writes ([`write_args`](POLICY-FIELDS.md#storage)). |
| `BACKUP_UPLOAD` | `admin` | `admin`: neo4j-admin writes to the bucket. `pipeline`: the tool uploads with `S3_SSE`. |
| `UPLOAD_STAGING_PATH` | `SCRATCH_PATH` | Local directory for `pipeline` mode. |

Use `pipeline` only for buckets that require an encryption header on PUTs. It needs
`RUNNER_MODE=subprocess`. AWS credentials come from the usual chain (instance role, IRSA, access keys).

??? note "Azure and GCS"

    | Variable | Default | Effect |
    |---|---|---|
    | `AZURE_STORAGE_CONNECTION_STRING` | unset | Azure credentials. |
    | `AZURE_STORAGE_ACCOUNT` | `devstoreaccount1` | Account name in the `azb://` URIs Neo4j seeds from. |
    | `AZURE_COPY_TIMEOUT` | `3600` | Seconds to wait for one server-side copy. |
    | `STORAGE_EMULATOR_HOST` | unset | fake-gcs-server address (tests only). |
    | `GCP_PROJECT` | `test` | Project for the emulator client, read only with `STORAGE_EMULATOR_HOST`. |

    Outside the emulator, GCS uses application default credentials.

## Runner (where neo4j-admin runs)

| Variable | Default | Effect |
|---|---|---|
| `RUNNER_MODE` | `subprocess` | `subprocess`: on the worker or CLI host. `k8s`: a pod per command. |
| `RUNNER_NEO4J_ADMIN` | `neo4j-admin` | Path to the binary (subprocess mode). |
| `RUNNER_EXEC_PREFIX` | `[]` | JSON argv before every command, e.g. `["docker","exec","-T","runner"]`. |
| `SCRATCH_PATH` | `/scratch` | Scratch directory, with room for the largest full. |
| `RUNNER_PAGECACHE` | `512M` | neo4j-admin `--pagecache`. |
| `RUNNER_HEAP_SIZE` | `2G` | neo4j-admin heap (`HEAP_SIZE`). |

??? note "k8s mode"

    | Variable | Default | Effect |
    |---|---|---|
    | `RUNNER_IMAGE` | unset | Neo4j Enterprise image of the cluster's version. |
    | `RUNNER_NAMESPACE` | Airflow: `default`; Dagster: the run pod's | Namespace for the pods. |
    | `RUNNER_IN_CLUSTER` | `true` | Airflow: use the in-cluster Kubernetes config. |
    | `RUNNER_SERVICE_ACCOUNT` | unset | The pods' service account (IRSA, workload identity). |
    | `RUNNER_MEMORY_LIMIT` | `4Gi` | Memory limit per pod. |
    | `RUNNER_MEMORY_REQUEST` / `RUNNER_CPU_REQUEST` | unset | Resource requests per pod. |
    | `RUNNER_SCRATCH_STORAGE` | `6Ti` | Ephemeral scratch for every pod. Size it for your largest full. |
    | `RUNNER_NODE_SELECTOR` | `{}` | JSON node selector. |
    | `RUNNER_TOLERATIONS` | `[]` | JSON list of tolerations. |
    | `RUNNER_SECRET_ENV` | `{}` | JSON `{"ENV_NAME": "<secret>:<key>"}`. Unmapped credentials are refused. |
    | `RUNNER_EXTRA_ENV` | `{}` | JSON env added to every pod as plain values. |

## Choosing a runner mode

Subprocess mode is simpler. The Dagster image or the Airflow `subprocess` image target carries
`neo4j-admin`. On Kubernetes, mount a ReadWriteMany volume (EFS, Filestore, Azure Files) in every
run pod. Size it for the full lane's limit times your largest full.

k8s mode starts a pod per command with its own scratch volume, so the worker image stays small.
It needs RBAC to create pods (`airflow/deploy/rbac.yaml`, for Dagster too) and a storage class for
ephemeral volumes. It refuses `BACKUP_UPLOAD=pipeline`.

Memory: `RUNNER_HEAP_SIZE` plus `RUNNER_PAGECACHE` plus about 1 GiB, as the worker's limit or `RUNNER_MEMORY_LIMIT`.

## Permissions

| Who | Needs |
|---|---|
| Dagster code server, Airflow scheduler and DAG processor | read the policy; the secrets |
| Dagster run pods, Airflow workers or task pods | bucket list, read, write, delete; KMS `GenerateDataKey` and `Decrypt`; Secrets Manager with `aws-sm`; Bolt (7687) |
| The same, in subprocess mode | the backup port (6362) |
| k8s runner pods (`RUNNER_SERVICE_ACCOUNT`) | the bucket and KMS as above; the backup port (6362) |
| Neo4j servers | bucket read and list; `kms:Decrypt` (they fetch seeds) |

## Behaviour

| Variable | Default | Effect |
|---|---|---|
| `NEO4J_BACKUP_REQUIRE_TICKET` | unset | `true`: every applied change needs a ticket. |
| `NEO4J_BACKUP_LOCK_WAIT` | `3600` | Seconds a job waits for its group's lock before failing with `locked`. |
| `NEO4J_BACKUP_ALERT_WEBHOOK` | unset | URL that failed runs are posted to as JSON `{"text": ...}`. |
| `DAGSTER_BASE_URL` | unset | Dagster UI URL, for links in alerts. |
| `NEO4J_BACKUP_MCP_MODE` | `read-only` | MCP server: `read-write` enables the guarded write tools. |
| `NEO4J_BACKUP_DEBUG` | unset | CLI: print the traceback of an unexpected error. |

The lock is `_locks/<group>.lock` in the default store, shared by every front-end. It expires after
6 hours; delete the object to clear it sooner.

??? note "Extension points"

    | Variable | Default | Effect |
    |---|---|---|
    | `CUTOVER_STRATEGY` / `CUTOVER_HOOK` | `alias-swap` / unset | `external` calls `CUTOVER_HOOK` (a URL or a command) in place of `ALTER ALIAS`. |
    | `PATH_LAYOUT` | unset | `module.Class`: your own bucket key layout. |
    | `NAMING` | unset | `module.Class`: your own database naming scheme. |
    | `NAMING_TS_FMT` | `%Y%m%dt%H%M%S` | strftime for the timestamp in new database names (no `-`). |
