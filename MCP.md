# Operator MCP server

`neo4j-backup-mcp` exposes the backup/restore operations as [MCP](https://modelcontextprotocol.io)
tools, so an operator can drive them through an agent: status questions and one-off restores
("what's the freshest backup for orders?", "restore orders to just before 14:32, preview
first"). Schedules belong to Dagster, Airflow or CI. It runs the same core as the
[CLI](CLI-CONTRACT.md); install it only if you want this.

## Safety model

Changes need the server started in read-write mode and `confirm=true` on the call:

1. **Scope.** The server starts **read-only** — only status and preview tools are exposed. Mutating
   tools appear only when you start it with `NEO4J_BACKUP_MCP_MODE=read-write`. Enabling mutation is
   a deliberate server-config decision, not something an agent can flip.
2. **Per-call confirmation.** Even in read-write, every mutating tool returns a `needs_confirmation`
   result (with the blast radius) unless called with `confirm=true`. Preview first with
   `dry_run=true`.
3. **Verify-before-drop.** A destructive `run_restore(replace=true)` verifies each backup is
   restorable *before* dropping anything (skip with `verify_first=false`).

Every tool call is logged (name, arguments, outcome) to stderr. Credentials come from the
environment, never from tool inputs. Transport is **stdio**: the agent spawns the server as a local
subprocess, so the process boundary is the trust boundary — do not put it on an unauthenticated
network transport with `read-write` enabled.

## Tools

Read-only (always available):

| Tool | Returns |
|---|---|
| `list_targets(group, tenant)` | every group/member the policy covers (or one group's or tenant's), with each group's tenant |
| `latest_artifact(group, member)` | the chain head for one member |
| `show_chain(group, member)` | the member's artifacts, oldest first, and its chains (full then diffs, per database folder) with the time window each covers, read from the artifact sidecars, and its load artifacts (`loads`, under `_seed/`) |
| `backup_status(tenant)` | every target's (or one tenant's) latest artifact and load, its age, tenant and cluster, and its state against `rpo_minutes`: `ok`, `stale` or `by_design`; `stale` lists the stale targets |
| `restore_points(group, tenant)` | per member: the current database, each chain's point-in-time window, load artifacts, gaps, and the restore command for each |
| `preview_restore(group, replace)` | the restore plan / blast radius, without mutating |
| `preview_prune` | the artifacts retention would delete, without deleting (whole chains only; `kept_for_chains`, `unknown_kept`) |

Mutating (only with `NEO4J_BACKUP_MCP_MODE=read-write`; each needs `confirm=true`):

| Tool | Does |
|---|---|
| `run_backup(group, kind, tenant)` | back up a group, or every group with a tenant label (additive) |
| `run_verify(group, tenant)` | consistency-check the latest backups (non-destructive) |
| `run_aggregate(group, confirm, dry_run, ticket, reason, tenant)` | collapse chains to a recovered full, in place |
| `run_restore(group, until, replace, confirm, dry_run, verify_first, baseline, ticket, reason, from_load, from_dump, members)` | restore (alias-swap / by-name, PITR), then a full backup of each restored database unless `baseline=false` |
| `run_prune(confirm, dry_run, ticket, reason)` | delete backups past retention |
| `run_rollback(group, members, confirm, dry_run, ticket, reason)` | point each alias back at its previous database |
| `run_adopt(group, migrate, replace, confirm, dry_run, ticket, reason)` | bring existing databases under the alias model from `overrides.<alias>.adopt_from` |
| `run_cleanup(group, keep, older_than_days, confirm, dry_run, ticket, reason)` | drop superseded databases the tool created, keeping the current one and `keep` before it |
| `run_restore_to_cluster(to_cluster, group, from_cluster, until, fence_source, skip_metadata, baseline, confirm, dry_run, ticket, reason)` | restore a group, or every group of a cluster, into another cluster |
| `run_load(group, confirm, dry_run, replace, keep_failed, baseline, reapply_access, ticket, reason)` | load or rebuild a group from its policy build: build, dump, seed, check, swap, baseline |

## Run it

Install the extra and point your agent at the server over stdio:

```bash
pip install "neo4j-backup-dagster[mcp] @ git+https://github.com/lex00/neo4j-backup@v0.6.0#subdirectory=orchestrator"
```

Agent client config (e.g. Claude Desktop / Claude Code `mcpServers`):

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

Same environment as the CLI (see the orchestrator README env table). Leave `NEO4J_BACKUP_MCP_MODE`
unset (or `read-only`) for status/planning; set `read-write` only where an operator should be able
to run guarded mutations. Mutating tools need `neo4j-admin` on the server's host, like the CLI.

A read-only round trip against the local stack is `just mcp-smoke`.
