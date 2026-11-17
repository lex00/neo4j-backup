# Restore on request

A restore needs the [group](../GLOSSARY.md) or [alias](../GLOSSARY.md), the time in UTC (or "latest") and a ticket.
Every database in the group moves to the same instant. Tell the customer that writes after that instant are lost.

=== "Agent (MCP)"

    Ask your agent: "restore demo to 09:00 UTC, ticket CHG-1234". Or run the prompt
    (Claude Code: `/mcp__neo4j-backup__restore_request demo 2026-11-15T09:00:00Z CHG-1234`).

    1. `restore_plan` shows each member's restore window, the plan and a message for the customer.
        If the time falls outside every backup, it lists the nearest windows instead.

    2. `preview_restore` shows the plan again. The agent stops and asks the operator.
    3. After the operator approves, `run_restore` with `confirm=true`, the ticket and a reason.
    4. The agent checks the baseline backup and returns `old` and `newdb` per member.

    The restore needs a read-write server. On a read-only server the agent returns the CLI command.

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

    Launch the `restore_group` job with `dry_run: true`, read the plan, then again with `dry_run: false`:

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
`NEO4J_BACKUP_REQUIRE_TICKET=true` makes an applied change require a ticket (exit 3, `ticket_required`).

## How the restore chooses and switches

- With `--until`, each member uses the [chain](../GLOSSARY.md) covering the time, even from an earlier database.
  If the time falls outside every chain, the restore stops before any change and lists the nearest windows.
- When `--until` is omitted, members go to the newest instant they all cover (`point` in the dry-run).
  When they lack a common instant, `alignment_warning` gives the reason and each member gets its newest backup.
- A member unchanged since the time is [seeded](../GLOSSARY.md) whole from its last backup.
- Each seed must be online on every server within the group's `rto_minutes`. Otherwise the restore fails before any alias moves.
- `--check` runs `load.checks` on the new databases first. A failure drops them (`--keep-failed` keeps them).
- Privileges on the old database are granted on the new one before the switch (`*` and `HOME` grants excluded).
- Aliases move one at a time. A partial failure (`cutover_partial`) lists what moved, and `rollback` undoes those.

To seed from a load artifact: `--from-load` (each member's newest dump in `_seed/`), or
`--from-dump <member>=<key-or-uri>`. Both conflict with `--until`.
A member with only loads is seeded from its newest load.

## Baseline backup

The restore ends with a full backup of each new database (`baseline` in the result), since a diff needs a full.
Skip it with `--no-baseline`, or `baseline: false` in Dagster, Airflow or MCP.

A failed baseline leaves the restore in place: exit 1, error `baseline_failed`.
Run `neo4j-backup backup <group> --kind FULL`. Until then, diffs report those members under `needs_full`.

## Undo a restore

Each swap is recorded in `_state/<group>.json`, and rollback reverts the latest one.
It requires the previous database. Privileges granted since the swap are copied back.
Running it again goes forward again.

=== "Agent (MCP)"

    `run_rollback(group, dry_run=true)`, then with `confirm=true` after the operator approves. The `restore_request` prompt offers it.

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
Cleanup drops only databases the tool created for a member and left unused by every alias.
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
