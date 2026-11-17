# Onboard a customer

## Add a group

1. Pick a tier. In the example policy, `gold` takes a daily full and hourly diffs.
    `silver` takes a weekly full and 6-hourly diffs, `bronze` a weekly full and daily diffs.
    The diff interval is what a restore can lose.

2. Put the database behind an alias (`CREATE ALIAS <alias> FOR DATABASE <database>`), or [adopt](#adopt-existing-databases) it.
3. Grant the customer's role on the database. A restore copies these grants to the new database.
4. Add a group to the policy:

    ```yaml
    - id: shop
      profile: app-live          # tier gold, 14 days; or data-interval, data-scratch, data-then-live
      aliases: [shop]
    ```

    Override profile values on the group (`retention_days: 30`). `policy show shop` shows the result.
    `policy validate <file>` checks the file (exit 0 valid, 2 invalid) with no cluster.

5. Ship the policy. Dagster's `reconcile_registry` sensor adds the targets within five minutes.
    Airflow picks them up at the next DAG parse.

6. For a new tier, switch on its schedules. Dagster creates them stopped unless the policy says `schedules: running`.
7. Take the first backup now and verify it:
    - CLI: `neo4j-backup backup <group> --kind FULL`, then `neo4j-backup verify <group>`.
    - Dagster: materialize the group's `backup` partitions with kind `FULL`, then `verify`.
    - Airflow: trigger `neo4j_backup_<tier>_full` with `{"group_id": "<group>"}`, then `neo4j_verify_<tier>`.

8. Tell the customer their tier and `retention_days`, and send them the [Customer guide](../CUSTOMERS.md).

A `tenant:` label on a customer's groups selects them all: `backup --tenant acme`, `verify --tenant acme`.
It is a label, not access control.

## Adopt existing databases

`adopt` puts existing databases behind the group's aliases. Name the database when it differs from the alias:

```yaml
- id: shop
  profile: app-live
  aliases: [shop]
  overrides:
    shop: {adopt_from: legacy-shop}
```

```bash
neo4j-backup --json adopt shop --dry-run      # per member: create, skip, conflict, migrate
neo4j-backup --json adopt shop --confirm      # CREATE ALIAS shop FOR DATABASE legacy-shop
```

Nothing is copied or dropped. A `conflict` (alias points elsewhere) stops it. MCP has `run_adopt`.

A database that already has the alias's name shows as `migrate`, since names and aliases share one namespace.
`--migrate --replace` moves it:

1. Sets the database read-only (writes fail until step 4), then backs it up and verifies it.
2. Seeds a new database from the backup, checks it and copies the privileges.
3. Drops the original.
4. Creates the alias with the old name for the new database.

!!! warning
    Schedule this as an outage and try it on a copy first. A failure before step 3 sets the original read-write again.

## Try a test group

Run the whole loop on a database nobody uses first:

```cypher
CREATE DATABASE `try-1` WAIT;
CREATE ALIAS `try` FOR DATABASE `try-1`;
```

```yaml
- id: try                      # in the policy, on any tier
  tier: gold
  aliases: [try]
```

Back it up and verify it (step 7), then preview a restore, restore it, and `rollback`.
Remove the group and drop the databases afterwards.
