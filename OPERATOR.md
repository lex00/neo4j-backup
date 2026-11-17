# Operator workflows

Six workflows cover the operator's work. Each has its own page, linked in the table below.
New to the tool: start with [Getting started](GETTING-STARTED.md). Terms are in the [glossary](GLOSSARY.md).

## How it fits together

- The [policy](POLICY.md) lists groups of databases. Each group has a tier (the backup schedule) and a retention period.
  A group restores to one instant.
- A runner with `neo4j-admin` pulls each backup into object storage. The database servers run only Neo4j.
- A restore seeds a new database from a backup and moves the tenant's alias to it.
  The old database stays until cleanup, so applications connect through the alias.

A scheduler (Dagster, Airflow or cron) runs the routine backups. Operators run the workflows by hand or through an agent.
Every change has a preview (`--dry-run`, `dry_run=true`) and an explicit confirm. Approvals follow your own change process.

## Workflows

Each workflow page has one tab per path: an agent on the [MCP server](MCP.md), the CLI, Dagster and Airflow.
The agent path follows the workflow's prompt: preview, operator approval, then the change.
In Dagster and Airflow, only the restore jobs take `dry_run`. Preview the other workflows with the agent or the CLI.

| Workflow | Agent prompt | CLI | Dagster | Airflow |
|---|---|---|---|---|
| [Onboard a customer](workflows/onboard.md) | `onboard` | `policy validate`, `adopt`, `backup`, `verify` | `backup`, `verify` assets | `neo4j_backup_<tier>_full` |
| [Check coverage](workflows/coverage.md) | `check_coverage` | `status`, `restore-points` | `status_check` | `neo4j_status` |
| [Restore on request](workflows/restore.md) | `restore_request` | `restore`, `rollback`, `cleanup` | `restore_group`, `rollback_job` | `neo4j_restore`, `neo4j_rollback` |
| [Load data in bulk](workflows/load.md) | `bulk_load` | `load`, `export` | `load_job` | `neo4j_load` |
| [Change or offboard](workflows/change.md) | `offboard` | `status`, `export`, `prune` | `<tier>_prune` | `neo4j_prune_<tier>` |
| [Recover or move](workflows/recover.md) | `recover_cluster` | `restore --to-cluster` | `move_job` | `neo4j_restore_to_cluster` |

## Reference

- Settings and hooks: [ENVIRONMENT.md](ENVIRONMENT.md).
- CLI output and exit codes: [CLI-CONTRACT.md](CLI-CONTRACT.md). Every command, for agents on the CLI: [AGENTS.md](AGENTS.md#commands).
