# Getting started

This page is for a platform team trying neo4j-backup for the first time. Part 1 runs the whole
loop on your laptop in about fifteen minutes: back up a group of databases, check the backups, and
restore them. Part 2 is the checklist for running it against your own clusters.

When you're done, the [Operator guide](OPERATOR.md) covers day-to-day work for your customers, and
the [Customer guide](CUSTOMERS.md) is the page to give the teams whose databases you run.

## Part 1: the whole loop, locally

### What you need

- Docker with Docker Compose
- [`just`](https://github.com/casey/just)
- Python 3.10 or newer (the Airflow venv, `just airflow-install`, also needs `uv`; it installs Python 3.13)

The stack runs Neo4j Enterprise under its evaluation license, the floci S3 emulator as the bucket,
and a separate runner container that holds `neo4j-admin`. Nothing is installed on the database
container to take backups.

### 1. Start the stack and the demo group

```bash
git clone https://github.com/lex00/neo4j-backup && cd neo4j-backup
just fresh
```

`just fresh` creates `.env` from `.env.example`, starts the containers, and creates the `demo` group
from [`policies/demo.yaml`](policies/demo.yaml): three databases, each behind an alias that
applications use.

```text
>> creating demo group 'demo' (alias-fronted; names via bootstrap/naming.sh)
>>   alias acme-orders -> acme-orders-20261115t204010
>>   alias acme-graph -> acme-graph-20261115t204010
>>   alias acme-audit -> acme-audit-20261115t204010
>> bootstrap complete. Try: just backup demo
```

The alias (`acme-orders`) is the stable name; the database behind it (`acme-orders-<timestamp>`)
changes every time you restore. That is how a restore can build a new database while the old one
keeps serving.

### 2. Install the CLI and point it at the stack

```bash
python3 -m venv orchestrator/.venv
orchestrator/.venv/bin/pip install -e 'orchestrator[dev]'

set -a; . ./.env; set +a
export NEO4J_BOLT_URI=neo4j://localhost:7687 \
       AWS_ENDPOINT_URL_S3=http://localhost:9000 \
       NEO4J_BACKUP_POLICY=policies/demo.yaml \
       BACKUP_UPLOAD=admin \
       RUNNER_EXEC_PREFIX='["docker","compose","--env-file",".env","-f","docker/compose.yaml","exec","-T","runner"]'
alias neo4j-backup=orchestrator/.venv/bin/neo4j-backup
```

The CLI runs on your machine and reaches Neo4j and the bucket through the published ports.
`RUNNER_EXEC_PREFIX` makes it run `neo4j-admin` inside the runner container, the way a production
runner sits next to the cluster rather than on it.

### 3. Back up, check, restore

```bash
neo4j-backup targets          # what the policy covers
neo4j-backup backup demo      # back up every database in the group
neo4j-backup verify demo      # prove the latest backups restore and are consistent
```

```text
ok: targets
{
  "targets": [
    "demo/acme-orders",
    "demo/acme-graph",
    "demo/acme-audit"
  ]
}
ok: verify [demo]
{
  "verified": [
    {
      "alias": "acme-orders",
      "physical": "acme-orders-20261115t204341",
      "consistent": true,
      "checked": 2
    },
    ...
```

Restore is guarded. Without a flag it refuses:

```text
$ neo4j-backup restore demo
error (confirm_required): restore mutates state; pass --confirm to apply or --dry-run to preview
```

`--dry-run` shows exactly which backup each database would come from and whether anything would be
dropped. `--confirm` does it:

```bash
neo4j-backup restore demo --dry-run
neo4j-backup restore demo --confirm
```

```text
seeded acme-orders-20261115t204747 <= demo/acme-orders/acme-orders-20261115t204341/acme-orders-20261115t204341-2026-11-15T20-46-55.backup
...
cutover acme-orders: acme-orders-20261115t204341 -> acme-orders-20261115t204747
cutover acme-graph: acme-graph-20261115t204341 -> acme-graph-20261115t204747
cutover acme-audit: acme-audit-20261115t204341 -> acme-audit-20261115t204747
```

Each alias now points at a new database seeded from the backup. The old databases are still there,
so undoing a restore is pointing the alias back (see the [Operator guide](OPERATOR.md#undo-a-restore)).

Neo4j stores privileges against the database, not the alias, so the restore re-grants every
privilege on each old database onto the new one before the switch. The demo uses the admin user,
whose grants are on `*`, so the restore output lists none.

### 4. Restore to a point in time

```bash
just demo-pitr
```

This builds a full backup, changes data, takes a differential backup, then restores once to the
time before the change and once to the latest state:

```text
>>   pitr-at-t0 (PITR to 2026-11-15T20:41:54.557Z): 2 customers
>>   pitr-head  (latest):       3 customers
>> PASS: point-in-time restore replayed the chain to T0 (2), HEAD is 3.
```

With the CLI it is `neo4j-backup restore demo --until 2026-11-15T20:41:54Z --confirm`. A point in
time needs a differential backup after a full one; a lone full can only be restored whole.

### 5. Look around, then clean up

```bash
just artifacts    # the backup files in the bucket
just urls         # Neo4j Browser and the S3 endpoint
just down         # stop everything and delete the volumes
```

The same loop runs through the other front-ends: `just cli-smoke`, `just airflow-smoke`, and
`orchestrator/smoke_local.py` for Dagster. [STACK.md](STACK.md) describes the stack, including how
the local S3 emulator handles encryption.

## Part 2: your own clusters

neo4j-backup runs next to your clusters, not on them. One policy file says what to back up and
when; a scheduler runs it; a runner with `neo4j-admin` does the backups; restores are Cypher over
Bolt.

Words used below (group, member, seed, tier and so on) are defined in the
[glossary](GLOSSARY.md).

### Choose how it runs

| Front-end | Use it when |
|---|---|
| [Dagster](orchestrator/README.md) | you run Dagster; you get backup lanes, retries and run history |
| [Airflow](airflow/README.md) | you run Airflow; same capabilities as a DAG set |
| [CLI](CLI-CONTRACT.md) with [CI or cron](CI.md) | a small number of databases and no orchestrator |
| [MCP server](MCP.md) | an operator drives status and restores through an agent; not for schedules |

All of them read the same policy and produce the same backups, so you can change your mind later.
On Kubernetes, the deploy kits are [Deploying on Dagster](orchestrator/deploy/DEPLOY.md) (image
and Helm values) and the Airflow page's [Deploying in
production](airflow/README.md#deploying-in-production) (worker image, RBAC, pod template).

### Prepare your Neo4j servers

Each Neo4j server needs three settings beyond a default install. The demo's
[`docker/compose.yaml`](docker/compose.yaml) sets the same ones.

1. Open the backup port to the runner. Neo4j listens for backups on localhost only by default:

   ```
   server.backup.enabled=true
   server.backup.listen_address=0.0.0.0:6362
   ```

   Allow the runner (the worker or pod that runs `neo4j-admin`) to reach port 6362 on the servers
   you back up from, usually the secondaries. The scheduler's workers need port 7687 (Bolt) on
   every server.
2. Let the servers read the bucket. A restore tells each server to fetch the backup itself
   (`CREATE DATABASE … OPTIONS {seedURI: …}`), using the server's own cloud access, not the
   runner's. On AWS give the servers an instance role or IRSA with `s3:GetObject` and
   `s3:ListBucket` on the bucket (and `kms:Decrypt` if it is encrypted), and set `AWS_REGION` in
   the server's environment. On S3-compatible storage also set `AWS_ENDPOINT_URL_S3`. On Azure
   (`azb://` seeds) set `AZURE_STORAGE_ACCOUNT` and give the servers a managed identity with
   Storage Blob Data Reader; on GCS (`gs://` seeds) set `GOOGLE_APPLICATION_CREDENTIALS` and
   `GOOGLE_CLOUD_PROJECT` or use workload identity with Storage Object Viewer.
3. Give the tool an admin user. It creates and drops databases, moves aliases and copies
   privileges, so it needs a user with the `admin` role (or the equivalent privileges on
   databases, aliases and privilege management). Put its password in `NEO4J_PASSWORD` or a
   secret reference.

### Checklist

1. Applications connect through aliases. Restore moves the alias to a new database. Existing
   databases can be put behind an alias without disruption with `neo4j-backup adopt`; see
   [Adopt existing databases](OPERATOR.md#adopt-existing-databases).
2. Write the policy. Copy [`policies/example.yaml`](policies/example.yaml): one group per
   customer (or per set of databases that must restore to the same instant), a tier for the
   schedule with its `rpo_minutes`, and `retention_days`. [POLICY.md](POLICY.md) has every field
   and how to choose them. Check it with `neo4j-backup policy validate <file>`.
3. Set the environment. With one cluster: `NEO4J_BOLT_URI`, `NEO4J_BACKUP_SOURCE` (a server's
   backup port, ideally a secondary), `NEO4J_PASSWORD` (or a secret reference),
   `BACKUP_BUCKET`, `AWS_REGION` and `NEO4J_BACKUP_POLICY`. With several clusters, the policy's
   `clusters:` block replaces the first two, and `NEO4J_PASSWORD` is used for any cluster
   without its own `password_ref`. On real AWS leave `AWS_ENDPOINT_URL_S3` unset. The full table
   is in [ENVIRONMENT.md](ENVIRONMENT.md).
4. Place the runner. It needs `neo4j-admin` (same version as the cluster), network access to
   the backup port (6362) and the bucket, and a scratch volume as large as your largest full
   backup. It runs as a subprocess on a worker or as a Kubernetes pod (`RUNNER_MODE=k8s`);
   [Choosing a runner mode](ENVIRONMENT.md#choosing-a-runner-mode) compares them.
5. Set concurrency. Dagster: the run limits in
   [`deploy/helm-values.yaml`](orchestrator/deploy/helm-values.yaml) (or
   [`deploy/dagster.yaml`](orchestrator/deploy/dagster.yaml) without Helm) and the `neo4j` pool.
   Airflow: create the pools listed on the [Airflow page](airflow/README.md#pools).
6. Try one group first; see [Try a test group](OPERATOR.md#try-a-test-group). Check the scratch
   size and the runner's memory while you do.
7. Turn on the schedules. In Dagster the `reconcile_registry` sensor and the schedules start
   stopped; switch them on together (or set `schedules: running` in the policy, which starts
   both). In Airflow, unpause the DAGs, the manual ones included. Every schedule comes from the
   policy: the tier crons and the `dbms:` block.
8. Turn on alerts. Set `NEO4J_BACKUP_ALERT_WEBHOOK` and failed runs post to it, including the
   freshness check that fails when a database's newest backup is older than its `rpo_minutes`.
   Add an independent check that works when the scheduler is down; see
   [Alerts](OPERATOR.md#alerts).

### What's planned

Open issues and plans are on [GitHub](https://github.com/lex00/neo4j-backup/issues).
