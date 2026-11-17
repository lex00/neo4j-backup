# Change or offboard a customer

## Change a tier

Change the group's `tier` in the policy and it moves to that [tier's](../GLOSSARY.md) schedules.
If the new tier's fulls are further apart than `retention_days`, backups are kept longer
(see [POLICY.md](../POLICY.md#choosing-tiers-retention-and-rpo)).

## Offboard

When a customer leaves, retire the group so its backups stay restorable for a set period:

```yaml
- id: shop
  profile: app-live
  aliases: [shop]
  state: retired
  retired_on: 2026-11-16      # the retention period starts here
  retire_after_days: 90       # default: the group's retention_days
```

Ship the policy. The group's schedules stop, and restores keep working.
From the `deletes_on` day, prune deletes the members' folders.

=== "Agent (MCP)"

    Run the `offboard` prompt with the group (Claude Code: `/mcp__neo4j-backup__offboard shop`). The agent:

    1. Checks with `policy_show` that the group is retired, and gives the `deletes_on` day from `coverage_report`.
    2. Offers a final portable dump per member with `run_export`, previewed first, if the customer wants their data.
    3. Shows what retention deletes now with `preview_prune`.

=== "CLI"

    ```bash
    neo4j-backup status shop                   # listed under retired, with deletes_on
    neo4j-backup --json export shop --dry-run  # then --confirm: a .dump per member in _seed/
    neo4j-backup --json prune --dry-run        # what retention deletes now
    ```

=== "Dagster"

    `<tier>_prune` deletes the folders from `deletes_on`.

=== "Airflow"

    `neo4j_prune_<tier>` deletes the folders from `deletes_on`.

Two steps stay manual. Drop the aliases and databases after the last restore you might need.
Once the data is gone, remove the group from the policy.

A group removed from the policy before retiring keeps its backups. `status` lists its folder under `unmanaged`.
