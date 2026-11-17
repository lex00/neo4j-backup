# Operator MCP server

`neo4j-backup-mcp` exposes backup and restore as [MCP](https://modelcontextprotocol.io) tools.
An operator can then ask an agent things like "restore orders to just before 14:32, preview first".
It runs the same core as the [CLI](CLI-CONTRACT.md) and returns the same results. It does not schedule anything.

## Safety model

1. The server starts read-only and exposes only status and preview tools.
    Mutating tools appear only with `NEO4J_BACKUP_MCP_MODE=read-write`, which an agent cannot set.

2. A destructive tool returns `needs_confirmation` with the blast radius unless called with `confirm=true`.
    Preview first with `dry_run=true`.

3. `run_restore(replace=true)` verifies each backup before dropping anything (skip with `verify_first=false`).

Every call is logged to stderr with its arguments and outcome. Credentials come from the environment, never from tool inputs.
The transport is stdio, so the agent's local subprocess boundary is the trust boundary.

!!! warning
    Do not put the server on an unauthenticated network transport with `read-write` enabled.

## Read-only tools

| Tool | Returns |
|---|---|
| `list_targets(group, tenant)` | The groups and members the policy covers, with each group's tenant |
| `latest_artifact(group, member)` | The chain head for one member |
| `show_chain(group, member)` | The member's artifacts and chains with their time windows, and its load artifacts |
| `backup_status(tenant)` | Each target's latest artifact and load, its age and its state: `ok`, `stale` or `by_design` |
| `restore_points(group, tenant)` | Per member: the current database, restore windows, loads, gaps and restore commands |
| `preview_restore(group, replace, until)` | The restore plan and blast radius, without changing anything |
| `preview_prune` | The artifacts retention would delete (whole chains only), without deleting |

## Mutating tools

These appear only with `NEO4J_BACKUP_MCP_MODE=read-write`.
`run_backup` and `run_verify` only add artifacts and run without `confirm`.
The rest need `confirm=true` and also take `dry_run`, `ticket` and `reason`, like the CLI flags.

| Tool | Does |
|---|---|
| `run_backup(group, kind, tenant)` | Back up a group, or every group of a tenant |
| `run_verify(group, tenant)` | Consistency-check the latest backups |
| `run_aggregate(group, tenant)` | Collapse chains into a recovered full, in place |
| `run_restore(group, until, replace, verify_first, baseline, from_load, from_dump, members)` | Restore, then take a full backup unless `baseline=false` |
| `run_prune()` | Delete backups past retention |
| `run_rollback(group, members)` | Point each alias back at its previous database |
| `run_adopt(group, migrate, replace)` | Put existing databases behind aliases from `overrides.<alias>.adopt_from` |
| `run_cleanup(group, keep, older_than_days)` | Drop superseded databases the tool created, keeping `keep` |
| `run_restore_to_cluster(to_cluster, group, from_cluster, until, fence_source, skip_metadata, baseline)` | Restore into another cluster |
| `run_load(group, replace, keep_failed, baseline, reapply_access)` | Load or rebuild a group from its policy build |

Restore steps are in [workflows/restore.md](workflows/restore.md). Loads are in [workflows/load.md](workflows/load.md).

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

`just mcp-smoke` runs a read-only round trip against the local stack.
