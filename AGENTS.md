# Driving neo4j-backup with an agent

This repo backs up and restores Neo4j Enterprise databases from a policy, over a shared core with
Dagster, Airflow, and CLI front-ends. The **`neo4j-backup` CLI is how an agent drives it** — no MCP
server required. Every command speaks the machine contract in [CLI-CONTRACT.md](CLI-CONTRACT.md):
`--json` gives one result object on stdout, and the exit code says what happened.

If you are an agent asked to back up, check, restore, or prune Neo4j here: read this file, then use
the CLI. Follow the safety rules below without exception.

## Safety posture

- **Read-only by default.** `targets`, `verify`, and any `--dry-run` are safe to run on your own.
- **Never mutate production without an explicit human go-ahead.** The mutating commands (`restore`,
  `prune`, `aggregate`, `metadata restore`) refuse to run unless you pass `--confirm`. Do not pass
  `--confirm` until a human has approved the specific action.
- **Always `--dry-run` a destructive command first**, show the human the blast radius it reports
  (databases to be dropped, artifact keys to be deleted, the restore plan), and only then propose
  the same command with `--confirm`.

## How to drive it

Invoke the installed console script (or `python -m neo4j_backup_cli`), always with `--json`:

```
neo4j-backup --json <command> [args]
```

Parse the envelope on stdout:

```json
{ "ok": true, "op": "verify", "group": "demo", "result": { ... }, "error": null }
```

- `ok: false` → stop and report `error.kind` / `error.msg`; do not retry blindly.
- Exit codes: `0` success, `1` the operation failed, `2` bad arguments, `3` a guard refused
  (you omitted `--confirm`, or a precondition failed). Exit `3` is not an error to work around — it
  means "get human confirmation" or "fix the precondition".
- Logs and neo4j-admin output go to **stderr**; stdout is only the JSON.

Configuration is environment + policy (`NEO4J_PASSWORD`, `NEO4J_BOLT_URI`, `BACKUP_BUCKET`, `CLOUD`,
`NEO4J_BACKUP_POLICY`, cloud credentials). See the README env table; do not invent flags for these.

## Command surface

| Command | What it does | Mutates? | Key flags |
|---|---|---|---|
| `targets` | List the policy's group/member targets | no | |
| `backup <group>` | Back up every database in a group | writes artifacts | `--kind AUTO\|FULL\|DIFF` |
| `verify <group>` | Consistency-check the latest backups (non-destructive) | no | |
| `aggregate <group>` | Collapse each chain into one recovered full, in place | **yes** | `--dry-run`, `--confirm` |
| `restore <group>` | Restore a group (alias-swap or by-name) | **yes** | `--until <iso>`, `--replace`, `--dry-run`, `--confirm` |
| `prune` | Delete backups past each group's retention | **yes** | `--dry-run`, `--confirm` |
| `metadata export` | Export users/roles/privileges/aliases as replayable Cypher | writes artifact | |
| `metadata restore` | Replay a metadata export into `system` | **yes** | `--key <k>`, `--dry-run`, `--confirm` |
| `system-backup` | Binary FULL backup of the `system` database | writes artifact | |

Restore detail: `--until <ISO-8601>` is point-in-time (needs a differential chain). `--replace` only
applies to by-name groups and **drops** an existing target before recreating it — the destructive
case; it appears in the dry-run plan's `drops`.

## Worked examples

Operator intent → the exact command. Prefer the safe/read-only form; escalate to `--confirm` only
after the human approves what the dry-run showed.

- "What can I back up here?" → `neo4j-backup --json targets`
- "Back up the demo group." → `neo4j-backup --json backup demo`
- "Is the latest demo backup restorable?" → `neo4j-backup --json verify demo`
- "Restore demo to just before 14:32 today — show me first."
  → `neo4j-backup --json restore demo --until 2026-07-11T14:32:00 --dry-run`
  then, after the human approves the plan, `neo4j-backup --json restore demo --until 2026-07-11T14:32:00 --confirm`
- "Clean up old backups — what would go?" → `neo4j-backup --json prune --dry-run`
  then, after approval, `neo4j-backup --json prune --confirm`

## More

- [CLI-CONTRACT.md](CLI-CONTRACT.md) — the envelope, exit-code table, and guard rules in full.
- [README.md](README.md) — what the project is, the env table, and the local stack.
- [RECOVERY.md](RECOVERY.md) — the human disaster-recovery runbook.
