# Neo4j Policy-Based Backup / Restore

### 📖 Documentation site: **<https://lex00.github.io/neo4j-backup/>**

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
keep them backed up and to restore them on request. One policy file describes every customer's
databases, schedules and retention, and drives every job: backups, verification, pruning,
loads, restores. Approvals stay with the team's own change process; the tool records the ticket
and reason (`--ticket`, `--reason`) and can refuse changes without one.

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

## No lock-in

It is orchestration around standard pieces:

- standard `neo4j-admin` backups in your bucket, restorable with `neo4j-admin` or a seed URI
  without this package;
- standard Cypher for restore (`CREATE DATABASE … seedURI`, `ALTER ALIAS`);
- a plain YAML policy, standard Neo4j aliases, a standard Dagster code location or Airflow DAGs;
- your own object store, IAM, KMS and runner placement.

Remove the tooling and you are left with standard Neo4j backups in your own bucket.

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
| [orchestrator/](orchestrator/README.md) | The Dagster code location, its configuration and environment table. |
| [airflow/](airflow/README.md) | The Airflow DAG set and how it maps to the Dagster one. |
| [DEPLOY.md](orchestrator/deploy/DEPLOY.md) | Adapting runner placement, concurrency and storage to your environment. |
| [DESIGN.md](DESIGN.md) | The architecture and the decisions behind it. |
| [STACK.md](STACK.md) | The local Docker stack. |
| [diagrams/](diagrams/README.md) | Architecture, storage layout, restore cutover, pipeline and naming diagrams. |
| [CHANGELOG.md](CHANGELOG.md) / [RELEASING.md](RELEASING.md) | Changes per version and how releases are cut. |

## What's validated

The full loop runs four ways against the local stack: shell (`just`), Dagster
(`orchestrator/smoke_*.py`), Airflow (`airflow/smoke_*.py`) and the CLI (`just cli-smoke`), plus
point-in-time recovery (`just demo-pitr`) and bulk import (`just import-smoke`). `RUNNER_MODE=k8s`
is validated on k3d for both adapters (`just k3d-smoke`, `just airflow-k8s-smoke`). Known issues
are listed in [Getting started](GETTING-STARTED.md#know-the-open-issues-first); decisions and open
risks are in [DESIGN.md §13](DESIGN.md).

## CI & docs site

- CI (`.github/workflows/ci.yml`): the unit tests, Dagster definitions validation, and the
  Airflow DAG import check on every push; no Docker or Neo4j needed.
- Docs (`.github/workflows/pages.yml`): this documentation and the rendered diagrams publish to
  GitHub Pages. Build locally with `just docs` (needs `pip install mkdocs-material`).
