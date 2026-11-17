# Connect an agent

The `neo4j-backup-mcp` server lets an agent run the [operator workflows](OPERATOR.md).
Ask "restore demo to just before 09:00, ticket CHG-1234" and the agent follows the restore prompt.
It previews each change and waits for operator approval. Results match the CLI's. Schedules stay with Dagster, Airflow or cron.

To see it on your laptop first, run `just try-mcp` ([Getting started](GETTING-STARTED.md)).

## Connect

Install the `mcp` extra:

```bash
pip install "neo4j-backup-dagster[mcp] @ git+https://github.com/lex00/neo4j-backup@v0.7.0#subdirectory=orchestrator"
```

Add the server to your agent client's `mcpServers` (in Claude Code, `.mcp.json` at the project root):

```json
{
  "mcpServers": {
    "neo4j-backup": {
      "command": "neo4j-backup-mcp",
      "env": {
        "NEO4J_BOLT_URI": "neo4j://db-host:7687",
        "NEO4J_BACKUP_SOURCE": "db-host:6362",
        "BACKUP_BUCKET": "your-bucket",
        "NEO4J_PASSWORD": "…",
        "AWS_ACCESS_KEY_ID": "…",
        "AWS_SECRET_ACCESS_KEY": "…",
        "NEO4J_BACKUP_POLICY": "policies/your.yaml",
        "NEO4J_BACKUP_MCP_MODE": "read-only"
      }
    }
  }
}
```

The server reads the same environment as the CLI ([ENVIRONMENT.md](ENVIRONMENT.md)).
Start read-only: the agent checks and plans, and hands you CLI commands for changes.
`NEO4J_BACKUP_MCP_MODE=read-write` lets it apply them, with `neo4j-admin` on the server's host.
`just mcp-smoke` runs a read-only round trip against the local stack: the prompts, `coverage_report` and `restore_plan`.

## Workflow prompts

Each workflow is a prompt: the tools to call in order, and where to stop for your yes.
Ask in plain words, or run a prompt by name (Claude Code: `/mcp__neo4j-backup__restore_request demo`).

| Prompt | Steps |
|---|---|
| `onboard(group, policy_file)` | validate the policy, show the group's settings, adopt, first full backup, verify ([onboard](workflows/onboard.md)) |
| `check_coverage(tenant)` | coverage report, then a backup for each stale member ([coverage](workflows/coverage.md)) |
| `restore_request(group, until, ticket)` | restore plan and customer message, restore, check the baseline ([restore](workflows/restore.md)) |
| `bulk_load(group, ticket)` | the group's load settings, load ([load](workflows/load.md)) |
| `offboard(group)` | check the group is retired, a final export, what prune deletes ([change](workflows/change.md)) |
| `recover_cluster(from_cluster, to_cluster, ticket)` | the groups that move, their freshness, restore into the other cluster ([recover](workflows/recover.md)) |

## An example run

From `just try-mcp`, after every `Order` in `acme-orders` was deleted (shortened):

```text
prompt restore_request(demo, 2026-11-17T21:05:31.163Z, TRY-1): 5 steps, operator approval before run_restore
mcp> restore_plan(group='demo', until='2026-11-17T21:05:31.163Z', ticket='TRY-1')
   acme-orders  restorable from 21:04:58 to 21:05:43
   customer message: We will restore acme-orders, acme-graph, acme-audit (demo) to
   2026-11-17T21:05:31.163Z (UTC). Changes made after that time will be lost. ...
mcp> preview_restore(group='demo', until='2026-11-17T21:05:31.163Z')
   acme-orders  replay to 21:05:31, then swap the alias from acme-orders-20261117t210424
   approved by operator: restore 3 databases to 21:05:31
mcp> run_restore(group='demo', until='2026-11-17T21:05:31.163Z', confirm=True, ticket='TRY-1', ...)
   acme-orders  alias acme-orders-20261117t210424 -> acme-orders-20261117t210618
   baseline full backups: 3
```

## Safety model

1. Read-only scope exposes the read tools only. Read-write is a server setting, outside the agent's reach.
2. A write tool applies its change only when called with `confirm=true`. Otherwise it returns `needs_confirmation` and the blast radius.
    Write tools also take `dry_run`, `ticket` and `reason`. `run_backup`, `run_verify` and `run_metadata_export` add artifacts and run directly.

3. `run_restore(replace=true)` verifies each backup before dropping anything.

Every call is logged to stderr with its arguments and outcome. Credentials come from the server's environment only.
The transport is stdio, so the agent's local subprocess is the trust boundary.

!!! warning
    Run a `read-write` server over stdio, or behind an authenticated transport.

## Tools

Write tools appear only in read-write scope.

### Look around

| Tool | Returns |
|---|---|
| `list_targets(group, tenant)` | The groups and members the policy covers, with each group's tenant and cluster |
| `policy_show(group)` | A group's effective settings and where each value came from |
| `latest_artifact(group, member)` | The chain head for one member |
| `show_chain(group, member)` | The member's artifacts and chains with their time windows, and its load artifacts |

### Onboard

| Tool | Scope | Does |
|---|---|---|
| `validate_policy(file)` | read | Check a `.yaml` policy file on the server's host; reads the file only |
| `preview_adopt(group, migrate, replace)` | read | Per member: create, skip, conflict or migrate |
| `run_adopt(group, migrate, replace)` | write | Put existing databases behind aliases from `overrides.<alias>.adopt_from` |
| `run_backup(group, kind, tenant)` | write | Back up a group, or every group of a tenant |
| `run_verify(group, tenant)` | write | Consistency-check the latest backups |

### Check coverage

| Tool | Scope | Does |
|---|---|---|
| `coverage_report(tenant, group)` | read | Members by state, with why each stale one is stale and the tool call that fixes it |
| `backup_status(tenant, group)` | read | Each target's latest artifact and load, its age and its state: `ok`, `stale` or `by_design` |
| `preview_prune()` | read | The artifacts retention would delete (whole chains only) |
| `run_prune()` | write | Delete backups past retention |
| `run_aggregate(group, tenant)` | write | Collapse chains into a recovered full, in place |

### Restore on request

| Tool | Scope | Does |
|---|---|---|
| `restore_plan(group, until, ticket)` | read | Restore points, the preview, `warnings` and a message for the customer; the nearest windows when backups miss `until` |
| `restore_points(group, tenant)` | read | Per member: the current database, restore windows, loads, gaps and restore commands |
| `preview_restore(group, replace, until)` | read | The restore plan and blast radius |
| `run_restore(group, until, replace, verify_first, baseline, from_load, from_dump, members)` | write | Restore, then take a full backup (skip with `baseline=false`) |
| `run_rollback(group, members)` | write | Point each alias back at its previous database |
| `run_cleanup(group, keep, older_than_days)` | write | Drop superseded databases the tool created, keeping `keep` |

### Load, export and recover

| Tool | Scope | Does |
|---|---|---|
| `run_load(group, replace, keep_failed, baseline, reapply_access)` | write | Load or rebuild a group from its policy build |
| `run_export(group, until)` | write | A portable dump per member in its `_seed/` folder; reads backups only |
| `run_metadata_export(cluster)` | write | Export users, roles, privileges and aliases as replayable Cypher |
| `run_restore_to_cluster(to_cluster, group, from_cluster, until, fence_source, skip_metadata, baseline)` | write | Restore into another cluster |

## Agents on the CLI

An agent that can run shell commands can drive the CLI with `--json` instead.
[AGENTS.md](AGENTS.md) is its instruction file: the safety rules, the JSON envelope and every command.
This repo also ships it as a Claude Code skill in `.claude/skills/neo4j-backup/`.
