# Operator workflows

For the platform team that runs Neo4j for other teams. When customers need a restore, they ask you.
If you haven't run the tool yet, start with [Getting started](GETTING-STARTED.md).

## How it fits together

- The [policy](POLICY.md) lists groups of databases, each with a tier (when backups run) and a retention.
  A group restores to one instant.
- A runner with `neo4j-admin` pulls each backup into the bucket. Nothing runs on the database servers.
- A restore seeds a new database from a backup and moves the customer's alias to it.
  The old database stays until you drop it, so applications must connect through the alias.
Schedulers run the routine backups. You run the six workflows below, by hand or through an agent.
Approvals stay with your own process. The tool's guard is a preview (`--dry-run`, `dry_run=true`) and an explicit confirm.

## Workflows

Each workflow page has a tab per path: an agent on the [MCP server](MCP.md), the CLI, Dagster and Airflow.
The agent path runs the workflow's prompt, which previews every change and waits for your yes.
Only the restore jobs preview in Dagster and Airflow (`dry_run`), so preview the others with the agent or the CLI.

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
- CLI output and exit codes: [CLI-CONTRACT.md](CLI-CONTRACT.md). Every command, for an agent without MCP: [AGENTS.md](AGENTS.md#commands).
