# Operator MCP server

`neo4j-backup-mcp` exposes the backup/restore operations as [MCP](https://modelcontextprotocol.io)
tools, so an operator drives them through an agent. Schedulers (Dagster/Airflow, or the
[CLI in CI](CI.md)) own the cadence; this server owns the **exceptions** — the disaster-recovery and
status questions you ask by hand: "what's the freshest backup for orders?", "restore orders to just
before 14:32, but show me the blast radius first."

It is thin over the same core as the [CLI](CLI-CONTRACT.md), and optional — install it only if you
want agent tool-calling on top of the CLI.

## Safety model

Three independent layers, so no single mistake mutates production:

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
| `list_targets` | every group/member the policy covers |
| `latest_artifact(group, member)` | the chain head for one member |
| `show_chain(group, member)` | the member's artifacts, oldest first |
| `backup_status` | every target's latest artifact and its age in hours |
| `preview_restore(group, replace)` | the restore plan / blast radius, without mutating |
| `preview_prune` | the artifacts retention would delete, without deleting |

Mutating (only with `NEO4J_BACKUP_MCP_MODE=read-write`; each needs `confirm=true`):

| Tool | Does |
|---|---|
| `run_backup(group, kind)` | back up a group (additive) |
| `run_verify(group)` | consistency-check the latest backups (non-destructive) |
| `run_aggregate(group, confirm, dry_run)` | collapse chains to a recovered full, in place |
| `run_restore(group, until, replace, confirm, dry_run, verify_first)` | restore (alias-swap / by-name, PITR) |
| `run_prune(confirm, dry_run)` | delete backups past retention |

## Run it

Install the extra and point your agent at the server over stdio:

```bash
pip install "neo4j-backup-dagster[mcp] @ git+https://github.com/lex00/neo4j-backup@v0.4.0#subdirectory=orchestrator"
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
