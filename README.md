# Neo4j Policy-Based Backup / Restore

Backups and restores for self-hosted Neo4j Enterprise, driven by one policy file.
For the platform team that runs Neo4j for the rest of its organization.

Documentation: <https://lex00.github.io/neo4j-backup/>

## How it works

1. The policy lists groups of databases, a tier for each, and how long backups are kept.
2. Dagster, Airflow, or the CLI from CI or cron runs `neo4j-admin` backups into your bucket.
    The database servers run only Neo4j.

3. A restore seeds a new database from the backup over Bolt, then moves the alias to it.
    Point-in-time restore and undo are built in.

Backups land under `<group>/<slug>/<database>/` in S3-compatible storage, Azure Blob or GCS.
They are plain `neo4j-admin` artifacts, restorable by hand with Cypher ([RECOVERY.md](RECOVERY.md)).

## Try it

Requirements: Docker, `just` and Python 3.10+. `just try` starts a local stack and backs up a demo group.
Then it deletes data and restores the group to the moment before the delete. `just try-mcp` runs the same restore through the MCP server.

## Start here

| You are | Read |
|---|---|
| Trying it for the first time | [Getting started](GETTING-STARTED.md): the full loop on a laptop |
| The platform team running it | [Operator workflows](OPERATOR.md): six jobs, each by agent, CLI, Dagster or Airflow |
| A tenant team whose databases the platform team hosts | [Customer guide](CUSTOMERS.md) |
| Writing the policy | [Policy](POLICY.md) and [every field](POLICY-FIELDS.md) |
| Running it through an agent | [Connect an agent](MCP.md) (MCP), or [AGENTS.md](AGENTS.md) for the CLI |

## Requirements

- Neo4j Enterprise, self-hosted. Online backup and seeding are Enterprise features; Community and Aura are out of scope.
- Applications connect through aliases, since a restore moves the alias.
- Command-line and scheduler tooling, adapted to your environment.

## Validation status

Every smoke runs against a local stack: Neo4j 2026.05 Enterprise in Docker, S3 emulated by floci.

| Area | Validated on | Next to validate |
|---|---|---|
| Neo4j topology | one server; a three-server cluster in Docker; two servers for a cross-cluster restore | a production cluster across hosts or zones |
| Object store | S3 API on floci; Azure on Azurite; GCS against fakes | real S3, Azure Blob and GCS; SSE-KMS with a real key |
| Orchestrators | Dagster 1.13 in-process and on its Helm chart in k3d; Airflow 3.2 with `dag.test` | Airflow on its Helm chart with KubernetesExecutor; Dagster+ |
| Runner | subprocess in a container; `RUNNER_MODE=k8s` on k3d for both orchestrators | k8s on a managed cluster with IRSA or workload identity |
| CLI and MCP | a smoke for every command (`just cli-smoke`, `ops-smoke`, `mcp-smoke`, ...) | |

## Development

CI runs the unit tests, Dagster definitions validation, the Airflow DAG import check and the docs prose lint.
The docs site builds with `just docs` and lints with `just docs-lint`.
Changes are in [CHANGELOG.md](CHANGELOG.md); releases follow [RELEASING.md](RELEASING.md).
Open issues are on [GitHub](https://github.com/lex00/neo4j-backup/issues).
