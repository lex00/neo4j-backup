# neo4j_backup_dagster

A Dagster code location for policy-driven Neo4j backup/restore. **Just tooling, no
lock-in:** it shells out to standard `neo4j-admin` and runs standard Cypher; artifacts
are ordinary `.backup` files in your bucket (restorable with `neo4j-admin` even without
this package), the policy is plain YAML, and aliases are standard Neo4j. Remove it and
you still have standard Neo4j backups.

Architecture and the decisions behind it: [`../DESIGN.md`](../DESIGN.md) §6.

## What's here

| Module | Role |
|---|---|
| `naming.py` | Naming authority — Python port of `bootstrap/naming.sh` (alias / slug / physical). Parity-tested. |
| `policy.py` | Pydantic models + loader for `policies/*.yaml`. |
| `resources.py` | `Neo4jResource` (Bolt restore), `ObjectStoreResource`, `RunnerResource` (neo4j-admin + subprocess/k8s mode). |
| `definitions.py` | The `Definitions`: backup / aggregate / verify / prune assets, restore job, schedules, sensor. |

## Prerequisite: applications connect via aliases

The restore model is an **alias swap** (seed a fresh physical → repoint a stable alias),
so apps must connect using a Neo4j **alias**, not a database name directly. The backup
asset also resolves the alias's current target, so **an alias must exist for each
database you back up**.

- New databases: create them behind an alias from the start.
- Existing databases your apps hit **directly**: adopt them with
  [`bootstrap/adopt.sh`](../bootstrap/adopt.sh) (see its header). A different alias name
  can point at the database with no disruption; reusing the *same* name (so apps don't
  change) requires a one-time migration (back up → restore into a uniquely-named
  physical → drop the original name → create the alias), because a database and an alias
  cannot share a name.

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
| `RUNNER_MODE` | `subprocess` | `subprocess` | `subprocess` or `k8s` |
| `NEO4J_BACKUP_POLICY` | `policies/demo.yaml` | demo | path to your policy |

k8s mode also reads `RUNNER_IMAGE`, `RUNNER_NODE_SELECTOR` (JSON),
`RUNNER_MEMORY_LIMIT`, `RUNNER_SCRATCH_STORAGE`, `RUNNER_SERVICE_ACCOUNT`.
AWS credentials come from the environment or an IAM role (no static keys needed on AWS).

## Execution modes

`neo4j-admin` (backup / aggregate / verify) runs via Dagster Pipes:

- `RUNNER_MODE=subprocess` (default, validated) — runs on the Dagster worker. The worker
  needs `neo4j-admin`, the scratch volume, network to 6362, and S3/KMS access. (Locally
  the smokes set `RunnerResource.exec_prefix` to run it in the demo container.)
- `RUNNER_MODE=k8s` — each backup runs in its own pod (`PipesK8sClient`) with a fresh
  ephemeral scratch PVC. Set `RUNNER_IMAGE` + the k8s vars above. Authored against the
  API; validate on your cluster.

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
