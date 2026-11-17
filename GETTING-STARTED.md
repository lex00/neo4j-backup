# Getting started

Part 1 runs the backup, check and restore loop on your laptop. Part 2 is the checklist for your own clusters.
Terms such as group and seed are in the [glossary](GLOSSARY.md).

## Part 1: try it in 15 minutes

You need Docker with Docker Compose, [`just`](https://github.com/casey/just) and Python 3.10 or newer.
The [local stack](STACK.md) runs Neo4j Enterprise, an S3 emulator and a runner with `neo4j-admin`.

1. Start the stack and the demo group:

     ```bash
     git clone https://github.com/lex00/neo4j-backup && cd neo4j-backup
     just fresh
     ```

     This creates the `demo` group from [`policies/demo.yaml`](policies/demo.yaml): three databases behind aliases.

     ```text
     >>   alias acme-orders -> acme-orders-20261115t204010
     >>   alias acme-graph -> acme-graph-20261115t204010
     >>   alias acme-audit -> acme-audit-20261115t204010
     >> bootstrap complete. Try: just backup demo
     ```

2. Install the CLI and point it at the stack:

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

     `RUNNER_EXEC_PREFIX` runs `neo4j-admin` inside the runner container.

3. Back up and check:

     ```bash
     neo4j-backup targets          # what the policy covers
     neo4j-backup backup demo      # back up every database in the group
     neo4j-backup verify demo      # prove the latest backups restore and are consistent
     ```

4. Restore. `--dry-run` shows which backup each database comes from, and `--confirm` does it:

     ```bash
     neo4j-backup restore demo --dry-run
     neo4j-backup restore demo --confirm
     ```

     ```text
     cutover acme-orders: acme-orders-20261115t204341 -> acme-orders-20261115t204747
     cutover acme-graph: acme-graph-20261115t204341 -> acme-graph-20261115t204747
     cutover acme-audit: acme-audit-20261115t204341 -> acme-audit-20261115t204747
     ```

     Each alias now points at a new database seeded from the backup, with privileges copied.
     The old databases stay, so you can undo ([Restore on request](workflows/restore.md)).

5. Restore to a point in time:

     ```bash
     just demo-pitr
     ```

     This takes a full, changes data, takes a differential, and restores to before and after the change.
     With the CLI it is `neo4j-backup restore demo --until 2026-11-15T20:41:54Z --confirm`.

6. Look around, then clean up:

     ```bash
     just artifacts    # the backup files in the bucket
     just urls         # Neo4j Browser and the S3 endpoint
     just down         # stop everything and delete the volumes
     ```

## Part 2: set up for production

One policy file says what to back up and when. A scheduler runs it, and a runner with `neo4j-admin` takes the backups.

### Choose how it runs

| Front-end | Use it when |
|---|---|
| [Dagster](orchestrator/README.md) | you run Dagster; you get backup lanes, retries and run history |
| [Airflow](airflow/README.md) | you run Airflow; the same work as a set of DAGs |
| [CLI](CLI-CONTRACT.md) with [CI or cron](CI.md) | a few databases and no orchestrator |
| [MCP server](MCP.md) | an operator drives status and restores through an agent; no schedules |

All read the same policy and write the same backups, so you can switch later. On Kubernetes, see [Deploying on Dagster](orchestrator/deploy/DEPLOY.md) or the Airflow page's
[Deploying in production](airflow/README.md#deploying-in-production).

### Prepare your Neo4j servers

The demo's [`docker/compose.yaml`](docker/compose.yaml) sets the same things.

1. Open the backup port (localhost only by default):

     ```
     server.backup.enabled=true
     server.backup.listen_address=0.0.0.0:6362
     ```

     The runner needs port 6362 on the servers you back up from. Workers need Bolt (7687) on every server.

2. Let every server read the bucket, since each fetches the seed itself. On AWS, give the servers an instance role or IRSA with `s3:GetObject` and
    `s3:ListBucket` (plus `kms:Decrypt` if encrypted), and set `AWS_REGION`. On S3-compatible
    storage also set `AWS_ENDPOINT_URL_S3`. For Azure (`azb://`), set `AZURE_STORAGE_ACCOUNT` and
    grant Storage Blob Data Reader. For GCS (`gs://`), set `GOOGLE_APPLICATION_CREDENTIALS` and
    `GOOGLE_CLOUD_PROJECT`, or use workload identity with Storage Object Viewer.

3. Create a user with the `admin` role (or equivalent) for the tool.
    Put its password in `NEO4J_PASSWORD` or a secret reference.

### Checklist

1. Put applications behind aliases. `neo4j-backup adopt` does this for existing databases
    without disruption ([Onboard a customer](workflows/onboard.md)).

2. Write the policy from [`policies/example.yaml`](policies/example.yaml) and [POLICY.md](POLICY.md).
    Check it with `neo4j-backup policy validate <file>`.

3. Set the environment ([ENVIRONMENT.md](ENVIRONMENT.md)). One cluster needs `NEO4J_BOLT_URI` and
    `NEO4J_BACKUP_SOURCE`, or the policy's `clusters:` block for several. All need `NEO4J_PASSWORD`,
    `BACKUP_BUCKET`, `AWS_REGION` and `NEO4J_BACKUP_POLICY`. On real AWS leave `AWS_ENDPOINT_URL_S3` unset.

4. Place the runner ([Choosing a runner mode](ENVIRONMENT.md#choosing-a-runner-mode)).
    It needs `neo4j-admin` at the cluster's version, port 6362, the bucket, and scratch for your largest full.

5. Set concurrency. For Dagster, see the run limits in
    [`deploy/helm-values.yaml`](orchestrator/deploy/helm-values.yaml) and the `neo4j` pool. For
    Airflow, create the [pools](airflow/README.md#pools).

6. Try one test group first ([Onboard a customer](workflows/onboard.md)), watching scratch and runner memory.
7. Turn on the schedules. In Dagster, start the `reconcile_registry` sensor and the schedules
    (or set `schedules: running`). In Airflow, unpause the DAGs.

8. Set `NEO4J_BACKUP_ALERT_WEBHOOK` for failed runs and freshness alerts.
    Add a check that works when the scheduler is down ([Check coverage](workflows/coverage.md)).

Day-to-day work is in the [Operator guide](OPERATOR.md). Give your customers the [Customer guide](CUSTOMERS.md).
