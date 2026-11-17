# Restore on request

An agent on the [MCP server](../MCP.md) runs this page with the `restore_request` prompt.

## Runbook

1. Get the group or alias, the time in UTC (or "latest") and a ticket. A restore moves every database in
    the group to the same instant. `--member <alias>` (repeatable) restores some only, and the dry-run warns.

2. List what can be restored:

    ```bash
    neo4j-backup --json restore-points <group>      # or --tenant <t>
    ```

    Per member: each chain's window (`from` to `to`), load artifacts and gaps. Read-only credentials are enough.

3. Tell the customer: writes after that time are gone, and transactions running at the switch must retry.
4. Preview:

    ```bash
    neo4j-backup restore <group> --dry-run                                   # latest
    neo4j-backup restore <group> --until 2026-11-15T09:00:00Z --dry-run      # point in time
    ```

5. Restore:

    ```bash
    neo4j-backup --json restore <group> --until 2026-11-15T09:00:00Z --confirm \
      --ticket CHG-1234 --reason "customer request: bad import at 09:05" > restore.json
    ```

    Dagster: the `restore_group` job with config
    `{ops: {restore_group_op: {config: {group_id: <group>, restore_until: "2026-11-15T09:00:00Z", ticket: CHG-1234, reason: "…"}}}}`.
    Airflow: `airflow dags trigger neo4j_restore -c '{"group_id": "<group>", "restore_until": "2026-11-15T09:00:00Z", "ticket": "CHG-1234", "reason": "…"}'`.

6. Check the baseline (below). Send the customer the restore point and `old` and `newdb` from the result.

Every mutating command and job records `--ticket` and `--reason`.
`NEO4J_BACKUP_REQUIRE_TICKET=true` refuses an applied change without one (exit 3, `ticket_required`).

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

```bash
neo4j-backup --json rollback <group> --dry-run          # [--member <alias>]
neo4j-backup --json rollback <group> --confirm --ticket CHG-1234
```

Each swap is recorded in `_state/<group>.json`, and rollback reverts the latest one.
It refuses if the previous database is gone. Privileges granted since the swap are copied back.
Running it again goes forward again. Dagster: `rollback_job`. Airflow: `neo4j_rollback`. MCP: `run_rollback`.

## Clean up later

After a soak period, drop the superseded databases:

```bash
neo4j-backup --json cleanup <group> --dry-run                 # the exact DROP set
neo4j-backup --json cleanup <group> --keep 1 --older-than 7 --confirm --ticket CHG-1234
```

Cleanup drops only databases the tool created for a member that no alias targets.
It keeps the current target and the `--keep` newest before it, and revokes copied privileges first.
It also runs on the tier's `cleanup_cron`.
