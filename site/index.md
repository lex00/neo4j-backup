---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

# Put a customer's Neo4j database back to any minute

You run Neo4j Enterprise for other teams. One policy file backs up all their databases.
When someone deletes the wrong data, you restore their group to just before it with one command.

</div>

A customer deleted every order in `acme-orders` just after 06:45:40 and asks for them back.
This is step 6 of `just try` on a laptop (the `acme-orders` lines):

```text
$ neo4j-backup restore-points demo
   acme-orders  any time from 06:45:04 to 06:45:55 (2 backups)
$ neo4j-backup restore demo --until 2026-11-17T06:45:40.032Z --dry-run
   acme-orders  replay to 06:45:40; seed a new database, then move the alias
$ neo4j-backup restore demo --until 2026-11-17T06:45:40.032Z --confirm --ticket TRY-1
   acme-orders  alias moved: acme-orders-20261117t064432 -> acme-orders-20261117t064630
   orders now: 2
```

The application keeps using the alias `acme-orders`, which now points at the restored copy.
The old database stays, so `neo4j-backup rollback demo` undoes the restore.

[Run this on your laptop in 15 minutes](GETTING-STARTED.md){ .md-button .md-button--primary }

## Adopt it in five steps

<div class="grid cards steps" markdown>

-   __1. Try it__

    ---

    Run `just try` on your laptop.
    Done when you restored the demo group.

    [Getting started](GETTING-STARTED.md)

-   __2. Write your policy__

    ---

    Start from `policies/example.yaml`.
    Done when `neo4j-backup policy validate <file>` exits 0.

    [Policy](POLICY.md)

-   __3. Back up one group__

    ---

    Point it at your cluster and back up one group.
    Done when `neo4j-backup status` shows it fresh.

    [Onboard a customer](workflows/onboard.md)

-   __4. Schedule it__

    ---

    Run it from Dagster, Airflow or cron.
    Done when backups run without you.

    [Dagster](orchestrator/deploy/DEPLOY.md), [Airflow](airflow/README.md), [CI](CI.md)

-   __5. Practice a restore__

    ---

    Walk through the runbook on a test group.
    Done when you restored the test group.

    [Restore on request](workflows/restore.md)

</div>

After that, the [operator guide](OPERATOR.md) covers the rest of the job: coverage checks, bulk loads, offboarding and lost clusters.

## Is it a fit?

You need:

- Neo4j Enterprise, self-hosted. Community and Aura are out of scope.
- S3-compatible storage, Azure Blob or GCS for the backups.
- Applications that connect through aliases, since a restore moves the alias.

The backups are plain `neo4j-admin` artifacts, so you can also [restore by hand](RECOVERY.md).
See [what's validated](STACK.md#whats-validated).

Also: the [customer guide](CUSTOMERS.md) to hand to your teams, and the CLI for
[agents](AGENTS.md), the [MCP server](MCP.md) and [CI](CI.md).
