# Change or offboard a customer

## Change a tier

Change the group's `tier` in the policy and it moves to that tier's schedules.
If the new tier's fulls are further apart than `retention_days`, backups are kept longer
(see [POLICY.md](../POLICY.md#choosing-tiers-retention-and-rpo)).

## Offboard

Retire the group instead of removing it:

```yaml
- id: shop
  profile: app-live
  aliases: [shop]
  state: retired
  retired_on: 2026-11-16      # the retention period starts here
  retire_after_days: 90       # default: the group's retention_days
```

1. Ship the policy. The group's schedules stop, but `restore` and `restore-points` still work.
2. `neo4j-backup status` lists the group under `retired` with its `deletes_on` day.
3. From that day, prune deletes the members' folders. `prune --dry-run` shows it first.
4. Drop the aliases and databases yourself after the last restore you might need.
5. Once the data is gone, remove the group from the policy.

A group removed without retiring keeps its backups. `status` lists its folder under `unmanaged`.
