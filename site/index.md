---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

# Backups for every Neo4j database your team runs

One policy file decides what is backed up, how often and for how long.
A restore seeds a new database and moves the alias once it is online.
For platform teams running self-hosted Neo4j Enterprise.

[Try it in 15 minutes](GETTING-STARTED.md){ .md-button .md-button--primary }
[See the operator workflows](OPERATOR.md){ .md-button }

</div>

## How it works

<div class="grid cards steps" markdown>

-   __1. Write a policy__

    ---

    Groups of databases, a tier for each, retention and recovery targets.
    [Policy guide](POLICY.md)

-   __2. Let it back up__

    ---

    Dagster, Airflow or cron runs `neo4j-admin` backups into your own bucket.
    Your servers run nothing extra.

-   __3. Restore on request__

    ---

    Seed a new database from the backup, check it, then move the alias.
    Point-in-time restore and undo are built in.

</div>

## Where to start

<div class="grid cards" markdown>

-   :material-laptop: __Trying it out__

    ---

    The whole loop on your laptop: back up, restore, restore to a point in time.

    [Getting started](GETTING-STARTED.md)

-   :material-server-network: __Running it for your organization__

    ---

    Onboarding, coverage checks, restores, bulk loads and recovery.

    [Operator guide](OPERATOR.md)

-   :material-account-group: __A customer of the platform team__

    ---

    What your backups give you and how to ask for a restore.

    [Customer guide](CUSTOMERS.md)

-   :material-robot-outline: __Driving it from an agent or CI__

    ---

    A CLI with JSON output and a dry-run guard, and an MCP server.

    [Agents](AGENTS.md), [MCP server](MCP.md), [CI recipes](CI.md)

</div>

## Is it a fit?

| You have | Supported |
|---|---|
| Neo4j Enterprise, self-hosted | Yes |
| Neo4j Community or Aura | No (no online backup, or managed snapshots) |
| Dagster, Airflow, or neither | Any of them; the CLI runs from CI or cron |
| S3-compatible storage, Azure Blob or GCS | Yes |
| Applications that connect through aliases | Needed for alias-swap restores |

The backups are plain `neo4j-admin` artifacts in your bucket.
You can restore them without this tool: [Recovery](RECOVERY.md) shows the Cypher.
Every command is tested end to end on a local stack; see [what's validated](STACK.md#whats-validated).
