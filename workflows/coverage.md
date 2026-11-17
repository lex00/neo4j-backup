# Check coverage

Coverage shows whether each customer database has a recent enough backup.
Each [member](../GLOSSARY.md) has a state: `ok`, `stale` (older than the group's `rpo_minutes`), `by_design` or `retired`.

=== "Agent (MCP)"

    Run the `check_coverage` prompt, optionally with a tenant (Claude Code: `/mcp__neo4j-backup__check_coverage`).
    It calls `coverage_report`, which groups members by state.
    Each stale member comes with a reason ("never backed up", or how far past `rpo_minutes`) and the tool call that fixes it.
    On a read-write server the agent runs those backups and checks again.

=== "CLI"

    ```bash
    neo4j-backup status                       # or --tenant <t>, or one group
    neo4j-backup status --stale               # exit 1 when any member is stale
    neo4j-backup restore-points <group>       # restore windows and gaps per member
    neo4j-backup verify <group>               # restore the latest backups aside and check them
    ```

=== "Dagster"

    The `status_check` schedule runs `status_job` on `dbms.status_cron` (default every 15 minutes) and fails when a member is stale.
    `<tier>_verify` runs verify on `verify_cron` (default daily 06:00).

=== "Airflow"

    The `neo4j_status` DAG runs on `dbms.status_cron` and fails when a member is stale.
    `neo4j_verify_<tier>` runs on `verify_cron`.

A diff for a member with a missing full is skipped, and the full lane takes over.
It shows as a warning on Dagster's `backup_taken` check and a skipped Airflow task.
Aggregate runs only on tiers that set `aggregate_cron`, since it removes point-in-time restore inside [chains](../GLOSSARY.md).

## Alerts

Set `NEO4J_BACKUP_ALERT_WEBHOOK` to a chat webhook and every failed run posts one line.
Dagster's `alert_on_failure` sensor and every Airflow DAG's failure callback send it.
Alerts stay off while the variable is unset (see the Airflow page's [Turn it on](../airflow/README.md#turn-it-on)).

These alerts need the scheduler. Add one check outside it, from cron, CI or a Kubernetes CronJob:

```bash
# every 30 minutes, from any host or pod with the CLI and read access to the bucket
*/30 * * * * neo4j-backup --json status --stale > /dev/null || notify "neo4j backups are stale"
```

`--stale` exits 1 when any member is stale. If the scheduler stops, members go stale and this fails.

## Changes customers make directly

A customer with cluster credentials can move an alias. Backups then follow the alias
to the other database, and its diffs are skipped until its first full.
When a group's backups start failing, compare `SHOW ALIASES` with your last restore result.
