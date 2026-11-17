---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

# Neo4j PITR for operations teams with many tenants

Backup and point-in-time restore for self-hosted Neo4j Enterprise, driven by one policy file.
A restore returns a tenant's group of databases to one instant and moves their aliases to the restored copies.

[Getting started](GETTING-STARTED.md){ .md-button .md-button--primary }
[Operator workflows](OPERATOR.md){ .md-button }

</div>

## Example: restore after a bad delete

Step 6 of `just try` (CLI) and `just try-mcp` (MCP) on a laptop.
Group `demo` holds three databases. Every `Order` node in `acme-orders` was deleted right after the `--until` time.
The two runs are separate, so their times differ.

=== "CLI"

    ```text
    $ neo4j-backup restore-points demo
       acme-orders  restorable from 21:08:04 to 21:08:49 (2 backups)
    $ neo4j-backup restore demo --until 2026-11-17T21:08:37.244Z --dry-run
       acme-orders  replay to 21:08:37, then swap the alias from acme-orders-20261117t210732
    $ neo4j-backup restore demo --until 2026-11-17T21:08:37.244Z --confirm --ticket TRY-1
       acme-orders  alias acme-orders-20261117t210732 -> acme-orders-20261117t210921
       orders now: 2
    ```

=== "MCP"

    ```text
    mcp> restore_plan(group='demo', until='2026-11-17T21:05:31.163Z', ticket='TRY-1')
       acme-orders  restorable from 21:04:58 to 21:05:43
    mcp> preview_restore(group='demo', until='2026-11-17T21:05:31.163Z')
       acme-orders  replay to 21:05:31, then swap the alias from acme-orders-20261117t210424
       approved by operator: restore 3 databases to 21:05:31
    mcp> run_restore(group='demo', until='2026-11-17T21:05:31.163Z', confirm=True, ticket='TRY-1')
       acme-orders  alias acme-orders-20261117t210424 -> acme-orders-20261117t210618
       orders now: 2
    ```

The alias `acme-orders` now targets the restored database.
The previous database stays available to `rollback` until cleanup drops it.

## Features

- Full and differential `neo4j-admin` backups on per-tier schedules, stored in S3, Azure Blob or GCS.
- Point-in-time restore of every database in a group to the same instant.
- Alias swap: applications keep their connection name, and `rollback` moves the alias back.
- Freshness checks against each group's RPO, with webhook alerts.
- Bulk loads, offboarding with retention, and restore into another cluster.
- Four front ends on one core: CLI, Dagster, Airflow, and an MCP server with a prompt per workflow.

## Rollout

1. Run `just try` on a laptop ([Getting started](GETTING-STARTED.md)).
2. Write a policy from `policies/example.yaml` and check it with `neo4j-backup policy validate <file>` ([Policy](POLICY.md)).
3. Back up one group from your cluster and confirm it with `neo4j-backup status` ([Onboard a customer](workflows/onboard.md)).
4. Schedule backups in [Dagster](orchestrator/deploy/DEPLOY.md), [Airflow](airflow/README.md) or [cron and CI](CI.md).
5. Optional: connect an MCP client, read-only first ([Connect an agent](MCP.md)).
6. Restore a test group and roll it back ([Restore on request](workflows/restore.md)).

## Requirements

- Neo4j Enterprise, self-hosted. Community and Aura are out of scope.
- S3-compatible storage, Azure Blob or GCS.
- Applications connected through aliases.

Backups are plain `neo4j-admin` artifacts, restorable by hand with Cypher ([Recovery](RECOVERY.md)).
Test coverage: [validation status](STACK.md#validation-status). For tenant teams: the [customer guide](CUSTOMERS.md).
