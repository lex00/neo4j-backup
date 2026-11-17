# Check coverage

An agent on the [MCP server](../MCP.md) runs this page with the `check_coverage` prompt.

## Status and restore points

- `neo4j-backup status` (or `--tenant <t>`, or one group) gives each member's newest restorable point.
  The state against `rpo_minutes` is `ok`, `stale`, or `by_design`. MCP: `coverage_report`.
- Dagster's `status_check` schedule and Airflow's `neo4j_status` DAG run it on `dbms.status_cron`
  (default every 15 minutes). They fail when a member is stale.
- A diff with no full to build on is skipped and the full lane takes over.
  It shows as a warning on Dagster's `backup_taken` check and a skipped Airflow task.
- `neo4j-backup verify <group>` combines the latest backups aside and runs a consistency check.
  It runs on each tier's `verify_cron` (default daily 06:00).
- `neo4j-backup restore-points <group>` shows what each member can be restored to, and gaps.

Aggregate runs only on tiers that set `aggregate_cron`, since it removes point-in-time restore inside chains.

## Alerts

Set `NEO4J_BACKUP_ALERT_WEBHOOK` to a chat webhook and every failed run posts one line.
That is Dagster's `alert_on_failure` sensor and every Airflow DAG's failure callback.
Without it nothing is sent (see the Airflow page's [Turn it on](../airflow/README.md#turn-it-on)).

These alerts need the scheduler. Add one check outside it, from cron, CI or a Kubernetes CronJob:

```bash
# every 30 minutes, from any host or pod with the CLI and read access to the bucket
*/30 * * * * neo4j-backup --json status --stale > /dev/null || notify "neo4j backups are stale"
```

`--stale` exits 1 when any member is stale. If the scheduler stops, members go stale and this fails.

## Changes customers make directly

A customer with cluster credentials can move an alias. Backups then follow the alias
to a database with no full, so its diffs are skipped until the next full.
When a group's backups start failing, compare `SHOW ALIASES` with your last restore result.
