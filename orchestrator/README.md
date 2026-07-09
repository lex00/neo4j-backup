# neo4j_backup_dagster

A Dagster code location for policy-driven Neo4j backup/restore. **Just tooling, no
lock-in:** it shells out to standard `neo4j-admin` and runs standard Cypher; artifacts
are ordinary `.backup` files in your bucket (restorable with `neo4j-admin` even without
this package), the policy is plain YAML, and aliases are standard Neo4j. Remove it and
you still have standard Neo4j backups.

Architecture and the decisions behind it: [`../DESIGN.md`](../DESIGN.md) §6. Prefer
Airflow? The [Airflow adapter](../airflow/README.md) is the equivalent DAG set over the
same core and policy.

## What's here

| Module | Role |
|---|---|
| `naming.py` | Naming authority — Python port of `bootstrap/naming.sh` (alias / slug / physical). Parity-tested. |
| `policy.py` | Pydantic models + loader for `policies/*.yaml`. |
| `resources.py` | `Neo4jResource` (Bolt restore), `ObjectStoreResource`, `RunnerResource` (neo4j-admin + subprocess/k8s mode). |
| `definitions.py` | The `Definitions`: backup / aggregate / verify / prune / metadata_export / system_backup assets, restore + metadata_restore jobs, schedules, sensor. |

## Configuration — what you edit, and where

There are **four** config surfaces. Everything else has a default. Start here.

| # | Controls | File you edit | What you set |
|---|---|---|---|
| 1 | **What to back up, and when** | a policy YAML — copy `../policies/demo.yaml` → `../policies/<you>.yaml` | your db groups, their `aliases`, a `tier` per group, `retention_days`; and the `tiers` (full/diff cron) |
| 2 | **Where your Neo4j and bucket are** | **environment variables** on the code location ([table below](#environment-variables)) | `NEO4J_PASSWORD` (only required one), `NEO4J_BOLT_URI`, `BACKUP_BUCKET`, `AWS_REGION`, `NEO4J_BACKUP_SOURCE`, and `NEO4J_BACKUP_POLICY` = path to #1 |
| 3 | **Backup concurrency (full/diff lanes)** | your Dagster instance's `dagster.yaml` — merge [`deploy/dagster.yaml`](deploy/dagster.yaml) | the `tag_concurrency_limits` |
| 4 | **Registering this package** | `workspace.yaml` (OSS) or `dagster_cloud.yaml` (Dagster+) — see [`deploy/DEPLOY.md`](deploy/DEPLOY.md) | one code-location entry → `neo4j_backup_dagster.definitions` |

Do them in order:

1. **Write your policy.** `cp policies/demo.yaml policies/prod.yaml`; edit each group's
   `id`, `aliases`, `tier`, `retention_days`, and the `tiers` cron. **The
   [Policy page](../POLICY.md) has a complete annotated example + every field.** Then set
   `NEO4J_BACKUP_POLICY=policies/prod.yaml`.
2. **Set the environment** (table below) so the code location reaches your Neo4j (Bolt
   7687 + backup port 6362) and your bucket. On AWS: leave `AWS_ENDPOINT_URL_S3` unset and
   use an IAM role; the only required secret is `NEO4J_PASSWORD`.
3. **Add the lanes:** merge `deploy/dagster.yaml` into your instance `dagster.yaml`.
4. **Register the code location** (snippet in `deploy/DEPLOY.md`), run
   `dagster definitions validate`, then enable the `reconcile_registry` sensor and the
   tier schedules (they default to STOPPED).

> None of it is hidden: #1 is a file you copy, #2 is env vars, #3 is a few lines in your
> existing `dagster.yaml`, #4 is one workspace entry. The full step-by-step (with the
> dry run and DB-node grants) is the [go-live checklist](#go-live-checklist-against-your-neo4j).

## Prerequisite: applications connect via aliases

The restore model is an **alias swap** (seed a fresh physical → repoint a stable alias),
so apps must connect using a Neo4j **alias**, not a database name directly. Backup resolves
each target to a physical database — an **alias** (→ its current target) or a physical
database name directly — so an alias isn't strictly required to *back up*, but it is for the
alias-swap *restore*.

- New databases: create them behind an alias from the start.
- Existing databases your apps hit **directly**: adopt them with
  [`bootstrap/adopt.sh`](../bootstrap/adopt.sh) (see its header). A different alias name
  can point at the database with no disruption; reusing the *same* name (so apps don't
  change) requires a one-time migration (back up → restore into a uniquely-named
  physical → drop the original name → create the alias), because a database and an alias
  cannot share a name.

## Policy

The policy is the foundation — a complete annotated example and the full field reference
are on the dedicated **[Policy page](../POLICY.md)**.

## Environment variables

Only `NEO4J_PASSWORD` is strictly required; the rest default sensibly.

| Var | Default | Local (MinIO) | Prod (AWS) |
|---|---|---|---|
| `NEO4J_PASSWORD` | — (required) | `devpassword` | your secret |
| `NEO4J_BOLT_URI` | `neo4j://localhost:7687` | local | `neo4j://<host>:7687` |
| `NEO4J_USER` | `neo4j` | `neo4j` | `neo4j` |
| `BACKUP_BUCKET` | `neo4j-backups` | `neo4j-backups` | your bucket |
| `AWS_ENDPOINT_URL_S3` | unset | `http://localhost:9000` | **leave unset** (real S3) |
| `AWS_REGION` | `us-east-1` | `us-east-1` | your region |
| `NEO4J_BACKUP_SOURCE` | `neo4j:6362` | `neo4j:6362` | `<follower>:6362` |
| `SCRATCH_PATH` | `/scratch` | `/scratch` | mounted volume path |
| `RUNNER_PAGECACHE` | `512M` | `512M` | size for your DBs |
| `RUNNER_HEAP_SIZE` | `2G` | `2G` | size for your DBs |
| `RUNNER_NEO4J_ADMIN` | `neo4j-admin` | `neo4j-admin` | path to the binary |
| `RUNNER_MODE` | `subprocess` | `subprocess` | `subprocess` or `k8s` |
| `NEO4J_BACKUP_POLICY` | `policies/demo.yaml` | demo | policy **path or `s3://bucket/key.yaml`** |

k8s mode also reads `RUNNER_IMAGE`, `RUNNER_NODE_SELECTOR` (JSON),
`RUNNER_MEMORY_LIMIT`, `RUNNER_SCRATCH_STORAGE`, `RUNNER_SERVICE_ACCOUNT`.
AWS credentials come from the environment or an IAM role (no static keys needed on AWS).

### Optional feature toggles

All default to today's behaviour; set only what you need. Shared by both adapters.

| Var | Default | What it does |
|---|---|---|
| `SECRET_PROVIDER` / `NEO4J_PASSWORD_REF` | `env` / — | Credential source: `env` reads `NEO4J_PASSWORD`; `aws-sm` fetches `NEO4J_PASSWORD_REF` (secret id/ARN, optional `#json_key`) per connect. |
| `SEED_CYPHER_VERSION` | unset | Pin the restore seed language. `5` emits `CYPHER 5 … existingData: 'use'` (required in Cypher 5); `25` omits it; unset = server default. Set to match your cluster. |
| `CUTOVER_STRATEGY` / `CUTOVER_HOOK` | `alias-swap` / — | Restore cutover. `alias-swap` (default) does `ALTER ALIAS`; `external` invokes `CUTOVER_HOOK` (http(s) URL or command) so an external router repoints. |
| `PATH_LAYOUT` | unset | Custom object-store key layout class (`module.Class`); unset = the default `<group>/<slug>/<physical>/` scheme. |
| `NEO4J_RETRY_ATTEMPTS` / `_BASE` / `_CAP` | `5` / `0.2` / `5.0` | Bounded exponential backoff for transient Bolt failures (leader re-election, dropped session, expired token). |
| `POLICY_CACHE_TTL` | `60` | Seconds to cache a policy loaded from `NEO4J_BACKUP_POLICY` (`0` = always fetch). Applies to an `s3://` source so edits are picked up without a redeploy; a failed re-read falls back to the last known good. The reconcile sensor always reads fresh. |
| `POLICY_LOADER` | unset | `module.callable` (`(source)->str`) overriding the built-in file/`s3://` fetch — for an **authenticated endpoint** / Vault / config API. Your loader does its own auth; validation, caching, and last-known-good still wrap it. |
| `S3_SSE` / `S3_SSE_KMS_KEY_ID` | unset | Explicit encryption header on the pipeline's boto3 PUT/COPY (metadata export, verify copy). Set `S3_SSE=aws:kms` (+ key id) for buckets that **require** it on PutObject; unset = bucket default. (neo4j-admin's `.backup` uploads are governed separately.) |
| `S3_WRITE_ARGS` | `{}` | JSON escape hatch merged into those PUT/COPY calls for any other arg (`BucketKeyEnabled`, `ACL`, …). |
| `BACKUP_UPLOAD` | `admin` | How neo4j-admin's S3 **writes** happen. `admin`: neo4j-admin writes straight to `s3://`. `pipeline`: neo4j-admin works on local disk and the pipeline does every S3 write via boto3 with `S3_SSE` — so **all** writes (backup, system, aggregate, verify) send the header, for buckets that **deny** header-less PutObject (neo4j-admin has no SSE setting). Reads stay direct. **Subprocess mode only.** |
| `UPLOAD_STAGING_PATH` | `SCRATCH_PATH` | Local dir neo4j-admin writes to in `pipeline` mode before upload (needs room for the artifact, like `--temp-path`). |

## Execution modes

`neo4j-admin` (backup / aggregate / verify) runs via Dagster Pipes:

- `RUNNER_MODE=subprocess` (default, validated) — runs on the Dagster worker. The worker
  needs `neo4j-admin`, the scratch volume, network to 6362, and S3/KMS access. (Locally
  the smokes set `RunnerResource.exec_prefix` to run it in the demo container.) **No k8s
  dependency** — `dagster-k8s` is not required for this mode.
- `RUNNER_MODE=k8s` — each backup runs in its own pod (`PipesK8sClient`) with a fresh
  ephemeral scratch PVC. Set `RUNNER_IMAGE` + the k8s vars above, and install the extra:
  `pip install 'neo4j-backup-dagster[k8s]'`. The import is lazy, so EC2/VM (subprocess)
  deployments load the code location without `dagster-k8s` installed; only this mode pulls
  it in. Authored against the API; validate on your cluster.

Restore is always pure Cypher over Bolt — no runner needed.

## Install & validate

```bash
pip install -e 'orchestrator[dev]'
pytest orchestrator/tests/                       # naming parity vs naming.sh
dagster definitions validate -m neo4j_backup_dagster.definitions
```

The local end-to-end smokes (against the `STACK.md` stack) are
`orchestrator/smoke_local.py`, `smoke_phase6.py`, `smoke_verify.py`.

## Go-live checklist (against your Neo4j)

1. **Aliases**: ensure apps use aliases; adopt existing databases (`bootstrap/adopt.sh`).
2. **Policy**: copy `policies/demo.yaml`, set your groups/aliases/tiers/retention; point
   `NEO4J_BACKUP_POLICY` at it.
3. **Env**: set the table above. On AWS leave `AWS_ENDPOINT_URL_S3` unset; use an IAM role.
4. **Runner**: subprocess → `neo4j-admin` on the worker; k8s → `RUNNER_MODE=k8s` +
   `RUNNER_IMAGE`. Mount a scratch volume sized to your largest full at `SCRATCH_PATH`.
5. **DB nodes**: grant S3 read + `kms:Decrypt` so seed-from-URI restore can pull.
6. **Instance**: merge the lanes from [`deploy/dagster.yaml`](deploy/dagster.yaml) into
   your `dagster.yaml` (coordinate — it's instance-global).
7. **Code location**: add the entry (`workspace.yaml` / `dagster_cloud.yaml`); pin
   `dagster` close to the host.
8. **Dry run**: materialize `backup` for one alias, then `verify`, then a test restore
   into a throwaway. Confirm scratch sizing + memory headroom.
9. **Enable**: turn on the `reconcile_registry` sensor and the tier schedules (they
   default to STOPPED).

See [`deploy/DEPLOY.md`](deploy/DEPLOY.md) for adapting placement to your environment.

## Parity contract

`naming.py` must match `bootstrap/naming.sh` exactly. `tests/test_naming_parity.py`
enforces it.
