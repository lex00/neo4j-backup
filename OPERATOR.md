# Operator guide

For the platform team that runs Neo4j for other teams. When customers need a restore, they ask you.
If you haven't run the tool yet, start with [Getting started](GETTING-STARTED.md).

## How it fits together

- The policy ([POLICY.md](POLICY.md)) lists groups of databases. A group restores to one instant.
  Each group has a tier (when backups run) and a retention.
- A scheduler runs the jobs: Dagster, Airflow, or cron/CI calling the CLI.
- A runner with `neo4j-admin` pulls each backup over the backup port into the bucket, under
  `<group>/<slug>/<database>/`. Nothing runs on the database servers.
- A restore creates a new database from a backup (`CREATE DATABASE … seedURI`), then moves the
  customer's alias to it. The old database stays until you drop it.

Applications must connect through the alias. The database name changes at every restore.
Approvals stay with your own process. The tool's guard is `--dry-run` to preview and `--confirm` to apply.

## Workflows

| Task | Page |
|---|---|
| Add a group, adopt existing databases, try a test group | [Onboard a customer](workflows/onboard.md) |
| Status, stale targets, alerts, changes customers make | [Check coverage](workflows/coverage.md) |
| The restore runbook, undo, clean up | [Restore on request](workflows/restore.md) |
| Build and swap a group from source files | [Load data in bulk](workflows/load.md) |
| Tier change, offboarding | [Change or offboard a customer](workflows/change.md) |
| Lost cluster, moving a group | [Recover or move a group](workflows/recover.md) |

## Reference

- Settings and hooks: [ENVIRONMENT.md](ENVIRONMENT.md).
- Every command: [AGENTS.md](AGENTS.md#commands). Output and exit codes: [CLI-CONTRACT.md](CLI-CONTRACT.md).
- The operator MCP server: [MCP.md](MCP.md).
