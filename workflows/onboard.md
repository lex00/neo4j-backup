# Onboard a customer

A team wants their databases backed up. You need the databases, the team's role, and a tier.
You add a group to the policy, put the databases behind aliases, and take the first backup.

## Add the group to the policy

1. Pick a tier. In the example policy, `gold` takes a daily full and hourly diffs.
    `silver` takes a weekly full and 6-hourly diffs, `bronze` a weekly full and daily diffs.
    The diff interval is what a restore can lose.

2. Grant the customer's role on the database. A restore copies these grants to the new database.
3. Add a group:

    ```yaml
    - id: shop
      profile: app-live          # tier gold, 14 days; or data-interval, data-scratch, data-then-live
      aliases: [shop]
    ```

    Override profile values on the group (`retention_days: 30`).

4. Ship the policy. Dagster's `reconcile_registry` sensor adds the targets within five minutes.
    Airflow picks them up at the next DAG parse. For a new tier, switch on its schedules:
    Dagster creates them stopped unless the policy says `schedules: running`.

## Check, alias and take the first backup

=== "Agent (MCP)"

    Run the `onboard` prompt with the group and the policy file
    (Claude Code: `/mcp__neo4j-backup__onboard shop policies/prod.yaml`). The agent:

    1. Checks the file with `validate_policy` and shows the group's tier and retention with `policy_show`.
    2. Previews aliases with `preview_adopt` and stops on a `conflict` or `migrate`.
        After your yes it runs `run_adopt`.

    3. Takes a full with `run_backup(kind="FULL")`, then `run_verify`.
    4. Confirms every member is `ok` in `coverage_report`.

=== "CLI"

    ```bash
    neo4j-backup policy validate policies/prod.yaml   # exit 0 valid, 2 invalid; no cluster needed
    neo4j-backup policy show shop                     # effective settings and where each came from
    neo4j-backup --json adopt shop --dry-run          # then --confirm, if a member needs an alias
    neo4j-backup backup shop --kind FULL
    neo4j-backup verify shop
    ```

=== "Dagster"

    Materialize the group's `backup` partitions with kind `FULL`, then `verify`.

=== "Airflow"

    Trigger `neo4j_backup_<tier>_full` with `{"group_id": "<group>"}`, then `neo4j_verify_<tier>`.

Then tell the customer their tier and `retention_days`, and send them the [Customer guide](../CUSTOMERS.md).

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

Nothing is copied or dropped. A `conflict` (alias points elsewhere) stops it.
MCP: `preview_adopt`, then `run_adopt`.

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

Back it up and verify it, then [restore it](restore.md) and roll it back.
Remove the group and drop the databases afterwards.
