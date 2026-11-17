# Restore on request

A customer wants their data back as it was at some moment. You need the group or alias,
the time in UTC (or "latest") and a ticket.
A restore moves every database in the group to the same instant, so tell the customer that writes after it are lost.

=== "Agent (MCP)"

    Ask your agent: "restore demo to 09:00 UTC, ticket CHG-1234". Or run the prompt
    (Claude Code: `/mcp__neo4j-backup__restore_request demo 2026-11-15T09:00:00Z CHG-1234`).

    1. `restore_plan` shows each member's restore window, the plan and a message for the customer.
        If no backup covers the time, it lists the nearest windows instead.

    2. `preview_restore` shows the plan again. The agent stops and asks you.
    3. After your yes, `run_restore` with `confirm=true`, the ticket and a reason.
    4. The agent checks the baseline backup and gives you `old` and `newdb` per member.

    The restore needs a read-write server. On a read-only one the agent hands you the CLI command.

=== "CLI"

    ```bash
    neo4j-backup --json restore-points <group>          # windows, loads, gaps per member
    neo4j-backup restore <group> --until 2026-11-15T09:00:00Z --dry-run
    neo4j-backup --json restore <group> --until 2026-11-15T09:00:00Z --confirm \
      --ticket CHG-1234 --reason "customer request: bad import at 09:05" > restore.json
    ```

    Leave out `--until` for the latest. `--member <alias>` (repeatable) restores some members only.
    `restore-points` works with read-only credentials.

=== "Dagster"

    Launch the `restore_group` job with `dry_run: true`, read the plan, then launch it again without:

    ```yaml
    ops:
      restore_group_op:
        config: {group_id: <group>, restore_until: "2026-11-15T09:00:00Z", ticket: CHG-1234, reason: "…"}
    ```

=== "Airflow"

    ```bash
    airflow dags trigger neo4j_restore -c '{"group_id": "<group>", "restore_until": "2026-11-15T09:00:00Z", "dry_run": true}'
    airflow dags trigger neo4j_restore -c '{"group_id": "<group>", "restore_until": "2026-11-15T09:00:00Z", "ticket": "CHG-1234", "reason": "…"}'
    ```

    Other Params are in the [Airflow page](../airflow/README.md#the-dags).

Afterwards, check the [baseline](#baseline-backup) and send the customer the restore point and the new database names.
Every path records the ticket and reason.
`NEO4J_BACKUP_REQUIRE_TICKET=true` refuses an applied change without a ticket (exit 3, `ticket_required`).

## How the restore chooses and switches

- With `--until`, each member uses the chain covering the time, even from an earlier database.
  If none covers it, nothing changes and the nearest windows are listed.
- Without `--until`, members go to the newest instant they all cover (`point` in the dry-run).
  If that's impossible, `alignment_warning` says why and each member gets its newest backup.
- A member with no change after the time is seeded whole from its last backup.
- Each seed must be online on every server within the group's `rto_minutes`, or the restore fails with no alias moved.
- `--check` runs `load.checks` on the new databases first. A failure drops them (`--keep-failed` keeps them).
- Privileges on the old database are granted on the new one before the switch (not `*` or `HOME` grants).
- Aliases move one at a time. A partial failure (`cutover_partial`) lists what moved, and `rollback` undoes those.

To seed from a load artifact: `--from-load` (each member's newest dump in `_seed/`), or
`--from-dump <member>=<key-or-uri>`. Neither goes with `--until`.
A member with no backups but a load is seeded from its newest load.

## Baseline backup

The restore ends with a full backup of each new database (`baseline` in the result), since a diff needs a full.
Skip it with `--no-baseline`, or `baseline: false` in Dagster, Airflow or MCP.

If it fails, the restore still stands: exit 1, error `baseline_failed`.
Run `neo4j-backup backup <group> --kind FULL`. Until then, diffs report those members under `needs_full`.

## Undo a restore

Each swap is recorded in `_state/<group>.json`, and rollback reverts the latest one.
It refuses if the previous database is gone. Privileges granted since the swap are copied back.
Running it again goes forward again.

=== "Agent (MCP)"

    `run_rollback(group, dry_run=true)`, then with `confirm=true` after your yes. The `restore_request` prompt offers it.

=== "CLI"

    ```bash
    neo4j-backup --json rollback <group> --dry-run          # [--member <alias>]
    neo4j-backup --json rollback <group> --confirm --ticket CHG-1234
    ```

=== "Dagster"

    The `rollback_job`, with `group_id` and optional `members`.

=== "Airflow"

    The `neo4j_rollback` DAG, with Params `group_id` and `members`.

## Clean up later

After a soak period, drop the superseded databases.
Cleanup drops only databases the tool created for a member that no alias targets.
It keeps the current target and the newest `keep` before it, and revokes copied privileges first.
It also runs on the tier's `cleanup_cron`.

=== "Agent (MCP)"

    `run_cleanup(group, keep, older_than_days, dry_run=true)` lists the exact DROP set. Apply with `confirm=true`.

=== "CLI"

    ```bash
    neo4j-backup --json cleanup <group> --dry-run                 # the exact DROP set
    neo4j-backup --json cleanup <group> --keep 1 --older-than 7 --confirm --ticket CHG-1234
    ```

=== "Dagster"

    The `cleanup_job`, with `group_id`, `keep` and `older_than_days`.

=== "Airflow"

    `neo4j_cleanup_<tier>` runs it for the whole tier.
