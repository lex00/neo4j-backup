# Neo4j Policy-Based Backup / Restore

Documentation site: <https://lex00.github.io/neo4j-backup/>

Policy-driven backup and restore for self-hosted Neo4j Enterprise, for the platform team that runs
Neo4j for the rest of its organization. The team edits one policy file; it decides what is backed up
for each customer, how often, and for how long. Restores are run by the team when a customer asks.

It runs as [Dagster](orchestrator/README.md) or [Airflow](airflow/README.md) pipelines, or as a
scheduler-agnostic CLI for CI and cron, all over one shared engine (`neo4j_backup_core`), and is
tested end to end against a real Neo4j Enterprise and object-store stack. The database servers run
nothing extra: backups are `neo4j-admin` run from a separate runner, and restores are Cypher over
Bolt (seed a new database from the backup, then move the alias). Backups land in your own bucket
under `<group>/<slug>/<database>/`, with your own IAM and KMS.

## Start here

| You are | Read |
|---|---|
| Trying it for the first time | [Getting started](GETTING-STARTED.md): the whole loop on your laptop, then the checklist for your clusters |
| The platform team running it | [Operator guide](OPERATOR.md): onboarding customers, checking coverage, the restore runbook, bulk loads, offboarding |
| A team whose database is run for you | [Customer guide](CUSTOMERS.md): what your backups give you, how to ask for a restore, what happens during one |

## Who this is for

A platform team that delivers Neo4j to the rest of its organization. Other teams (the
customers) run their applications on databases the platform team operates, and depend on it to
keep them backed up and to restore them on request. Approvals stay with the team's own change
process; the tool records the ticket and reason (`--ticket`, `--reason`) and can refuse changes
without one.

Such teams:

- run **Neo4j Enterprise** themselves (online backup and seeding a database from a backup are
  Enterprise features; Community has only offline dump and load);
- run **Dagster** or **Airflow**, or neither: the CLI drives the same engine from CI or cron
  ([CI recipes](CI.md));
- store backups in an **S3-compatible bucket**, Azure Blob or GCS (`CLOUD=azure|gcp`);
- have applications connect through Neo4j **aliases**, or are willing to move them to aliases (a
  restore moves the alias to the restored database).

Not for Neo4j Community (no online backup), Neo4j Aura (managed, with its own snapshots), or anyone
wanting a turnkey GUI product. This is tooling to adapt to your environment.

### Front-ends: pick by what you already run

| Front-end | Pick when |
|---|---|
| [Dagster](orchestrator/README.md) / [Airflow](airflow/README.md) | you run an orchestrator and want concurrency lanes, the policy's groups as targets, retries with backoff, and run history |
| [`neo4j-backup` CLI](OPERATOR.md#commands) + [CI](CI.md) / cron | a small fleet with no orchestrator; you accept CI's limits (scratch space, best-effort cron, no orchestration) for a much lighter setup |
| [MCP server](MCP.md) | an operator drives status and restores through an agent; read-only by default, changes guarded; for exceptions, not schedules |

The policy, the bucket layout and every setting are the same across them.

## Without the tool

The backups are ordinary `neo4j-admin` artifacts in your bucket, and a restore is two Cypher
statements (`CREATE DATABASE … OPTIONS { seedURI }`, `ALTER ALIAS`); [RECOVERY.md](RECOVERY.md)
shows them, so you can restore without this package.

## Documentation map

| Doc | What |
|---|---|
| [GETTING-STARTED.md](GETTING-STARTED.md) | The local loop in fifteen minutes, then the production checklist. |
| [OPERATOR.md](OPERATOR.md) | Runbooks for the platform team, settings, and the command list. |
| [CUSTOMERS.md](CUSTOMERS.md) | The page to give your customers. |
| [POLICY.md](POLICY.md) | The policy: an annotated example and every field. |
| [RECOVERY.md](RECOVERY.md) | The recovery modes (full, differential, point in time) with the exact Cypher. |
| [IMPORT.md](IMPORT.md) | Bulk import from CSV or Parquet on a separate loader. |
| [CLI-CONTRACT.md](CLI-CONTRACT.md) | The CLI's JSON output, exit codes, and the dry-run/confirm guard. |
| [CI.md](CI.md) | Scheduling the CLI from GitHub, GitLab or Forgejo. |
| [MCP.md](MCP.md) | The operator MCP server. |
| [AGENTS.md](AGENTS.md) | Driving the CLI from a coding or ops agent. |
| [ENVIRONMENT.md](ENVIRONMENT.md) | Every environment variable, with its default. |
| [orchestrator/](orchestrator/README.md) | The Dagster code location: what it defines and its configuration. |
| [airflow/](airflow/README.md) | The Airflow DAGs, and deploying them in production. |
| [DEPLOY.md](orchestrator/deploy/DEPLOY.md) | Deploying the Dagster code location in production. |
| [DESIGN.md](DESIGN.md) | Design notes and history; parts describe plans that changed. |
| [STACK.md](STACK.md) | The local Docker stack. |
| [diagrams/](diagrams/README.md) | Architecture, storage layout, restore cutover, pipeline and naming diagrams. |
| [CHANGELOG.md](CHANGELOG.md) / [RELEASING.md](RELEASING.md) | Changes per version and how releases are cut. |

## What's validated

Every smoke runs against a local stack: Neo4j 2026.05 Enterprise in Docker, S3 emulated by
floci. What that covers, and what it doesn't:

| Area | Validated on | Not validated |
|---|---|---|
| Neo4j topology | single server; two independent single servers for restore into another cluster | a multi-server cluster (backup from a secondary, seeding with `TOPOLOGY`, the alias swap seen by every member) |
| Object store | S3 API on floci; Azure on Azurite (store operations; Neo4j can't seed from an emulator); GCS against fakes | real AWS S3, Azure Blob and GCS; SSE-KMS with a real KMS key (checked on MinIO before 2026-11) |
| Orchestrators | Dagster 1.13 in-process (`execute_in_process`, `materialize`); Airflow 3.2 with `dag.test` | a deployed Dagster instance or Airflow scheduler with real run launchers |
| Runner | subprocess (in a container); `RUNNER_MODE=k8s` on k3d for both orchestrators (backups, an import build, an image build) | k8s on a managed cluster with IRSA or workload identity |
| CLI and MCP | every command's smoke (`just cli-smoke`, `ops-smoke`, `move-smoke`, `load-smoke`, `mcp-smoke`, ...) | |

Open issues are on [GitHub](https://github.com/lex00/neo4j-backup/issues).

## CI & docs site

- CI (`.github/workflows/ci.yml`): the unit tests, Dagster definitions validation, and the
  Airflow DAG import check on every push; no Docker or Neo4j needed.
- Docs (`.github/workflows/pages.yml`): this documentation and the rendered diagrams publish to
  GitHub Pages. Build locally with `just docs` (needs `pip install mkdocs-material`).
