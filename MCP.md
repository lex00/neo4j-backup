# Operator MCP server

`neo4j-backup-mcp` exposes backup and restore as [MCP](https://modelcontextprotocol.io) tools.
An operator can then ask an agent things like "restore orders to just before 14:32, preview first".
It runs the same core as the [CLI](CLI-CONTRACT.md) and returns the same results. It does not schedule anything.

## Workflows

Each operator workflow is an MCP prompt. The agent gets the steps: which tools to call and in what order.
It also learns where to stop for the operator's yes.
In read-only scope, a step that changes something gives the operator the CLI's `--dry-run` and `--confirm` commands instead.

| Prompt | Steps |
|---|---|
| `onboard(group, policy_file)` | validate the policy, show the group's settings, adopt, first full backup, verify ([onboard](workflows/onboard.md)) |
| `check_coverage(tenant)` | coverage report, then a backup for each stale member ([coverage](workflows/coverage.md)) |
| `restore_request(group, until, ticket)` | restore plan and customer message, restore, check the baseline ([restore](workflows/restore.md)) |
| `bulk_load(group, ticket)` | the group's load settings, load ([load](workflows/load.md)) |
| `offboard(group)` | check the group is retired, a final export, what prune deletes ([change](workflows/change.md)) |
| `recover_cluster(from_cluster, to_cluster, ticket)` | the groups that move, their freshness, restore into the other cluster ([recover](workflows/recover.md)) |

## Safety model

1. The server starts read-only and exposes only status and preview tools.
    Mutating tools appear only with `NEO4J_BACKUP_MCP_MODE=read-write`, which an agent cannot set.

2. A destructive tool returns `needs_confirmation` with the blast radius unless called with `confirm=true`.
    Preview first with `dry_run=true`.
    `run_backup`, `run_verify` and `run_metadata_export` only add artifacts and run without `confirm`.

3. `run_restore(replace=true)` verifies each backup before dropping anything (skip with `verify_first=false`).

Guarded tools also take `dry_run`, `ticket` and `reason`, like the CLI flags.
Every call is logged to stderr with its arguments and outcome. Credentials come from the environment, never from tool inputs.
The transport is stdio, so the agent's local subprocess boundary is the trust boundary.

!!! warning
    Do not put the server on an unauthenticated network transport with `read-write` enabled.

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
| `validate_policy(file)` | read | Check a `.yaml` policy file on the server's host, with no cluster |
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
| `restore_plan(group, until, ticket)` | read | Restore points, the preview, `warnings` and a message for the customer; the nearest windows if `until` isn't covered |
| `restore_points(group, tenant)` | read | Per member: the current database, restore windows, loads, gaps and restore commands |
| `preview_restore(group, replace, until)` | read | The restore plan and blast radius |
| `run_restore(group, until, replace, verify_first, baseline, from_load, from_dump, members)` | write | Restore, then take a full backup unless `baseline=false` |
| `run_rollback(group, members)` | write | Point each alias back at its previous database |
| `run_cleanup(group, keep, older_than_days)` | write | Drop superseded databases the tool created, keeping `keep` |

### Load, export and recover

| Tool | Scope | Does |
|---|---|---|
| `run_load(group, replace, keep_failed, baseline, reapply_access)` | write | Load or rebuild a group from its policy build |
| `run_export(group, until)` | write | A portable dump per member in its `_seed/` folder; never touches the cluster |
| `run_metadata_export(cluster)` | write | Export users, roles, privileges and aliases as replayable Cypher |
| `run_restore_to_cluster(to_cluster, group, from_cluster, until, fence_source, skip_metadata, baseline)` | write | Restore into another cluster |

## Run it

Install the `mcp` extra:

```bash
pip install "neo4j-backup-dagster[mcp] @ git+https://github.com/lex00/neo4j-backup@v0.7.0#subdirectory=orchestrator"
```

Add it to your agent client's `mcpServers`:

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
Mutating tools need `neo4j-admin` on the server's host.

`just mcp-smoke` runs a read-only round trip against the local stack: the prompts, `coverage_report` and `restore_plan`.
