# Driving neo4j-backup with an agent

This repo backs up and restores Neo4j Enterprise databases from a policy.
An agent drives it through the `neo4j-backup` CLI, which follows [CLI-CONTRACT.md](CLI-CONTRACT.md).
Follow the safety rules below without exception.

## Safety rules

- Read-only commands and any `--dry-run` are safe to run on your own.
- Never pass `--confirm` until a human has approved that exact action.
  Mutating commands refuse to run without it.
- Run a destructive command with `--dry-run` first and show the human the plan. Then propose it with `--confirm`.

## How to call it

Always pass `--json`:

```
neo4j-backup --json <command> [args]
```

Parse the envelope on stdout:

```json
{ "ok": true, "op": "verify", "group": "demo", "result": { ... }, "error": null }
```

- On `ok: false`, stop and report `error.kind` and `error.msg`. Do not retry blindly.
- Exit codes: `0` success, `1` failed, `2` bad arguments, `3` a guard refused.
- Exit `3` means "get human confirmation" or "fix the precondition". Do not work around it.
- Logs and neo4j-admin output go to stderr.

Configuration comes from the environment and the policy file ([ENVIRONMENT.md](ENVIRONMENT.md)). Do not invent flags for it.

## Commands

The read commands, `backup` and `aggregate` take `--tenant <t>` in place of a group.
Commands marked "yes" (and `export`) take `--dry-run`, `--confirm`, `--ticket` and `--reason`.
Run `neo4j-backup <command> --help` for all flags.

| Command | What it does | Mutates? |
|---|---|---|
| `targets [<group>]` | List the policy's groups and members | no |
| `status [<group>]` | Freshness against `rpo_minutes`; `--stale` exits 1 if any target is stale | no |
| `restore-points <group>` | What each member can be restored to | no |
| `policy show <group>` | A group's effective settings and where each came from | no |
| `policy validate <file>` | Validate a policy file; exit `2` if invalid | no |
| `policy check` | Probe every store the policy uses | probe only |
| `verify <group>` | Consistency-check the latest backups | no |
| `backup <group>` | Back up every database in a group (`--kind AUTO\|FULL\|DIFF`) | adds artifacts |
| `export <group>` | A `.dump` per member at `--until` or the latest backup | adds artifacts |
| `metadata export` | Export users, roles and aliases as Cypher | adds artifact |
| `system-backup` | Full binary backup of the `system` database | adds artifact |
| `import <database>` | Offline bulk import with `neo4j-admin` ([IMPORT.md](IMPORT.md)) | creates a store |
| `aggregate <group>` | Collapse each chain into one recovered full, in place | yes |
| `restore <group>` | Restore a group; see below | yes |
| `rollback <group>` | Point each alias back at its database before the last swap | yes |
| `cleanup <group>` | Drop superseded databases the tool created (`--keep N`) | yes |
| `adopt <group>` | Put existing databases behind the group's aliases | yes |
| `load <group>` | Load or rebuild a group from its policy build | yes |
| `prune` | Delete backups past each group's retention | yes |
| `metadata restore` | Replay a metadata export into `system` | yes |

Backup: each entry in `result.backups` has `taken` (`full` or `diff`).
A `--kind DIFF` for a database with no full yet is skipped and listed in `result.needs_full`.
Follow up with `--kind FULL`.

Restore:

- `--until <ISO-8601>` restores to a point in time and needs a diff chain. A time with no offset is UTC.
- After a restore the CLI takes a full backup of each restored database (`--no-baseline` skips it).
  If that fails, the restore stays applied with error kind `baseline_failed`. Rerun `backup <group> --kind FULL`.
- `--replace` (by-name groups only) drops the existing database first. The dry-run lists it under `drops`.
- See [workflows/restore.md](workflows/restore.md) for `--member`, `--check` and the cluster, load and dump flags.

## Examples

| Operator asks | Command |
|---|---|
| "What can I back up here?" | `neo4j-backup --json targets` |
| "Is anything stale?" | `neo4j-backup --json status --stale` |
| "Back up the demo group." | `neo4j-backup --json backup demo` |
| "Is the latest demo backup restorable?" | `neo4j-backup --json verify demo` |
| "Restore demo to just before 14:32, show me first." | `neo4j-backup --json restore demo --until 2026-07-11T14:32:00 --dry-run` |
| After approval | `neo4j-backup --json restore demo --until 2026-07-11T14:32:00 --confirm` |
| "What would pruning delete?" | `neo4j-backup --json prune --dry-run` |

## See also

The [MCP server](MCP.md) offers the same operations as typed tools. Operator tasks are in [OPERATOR.md](OPERATOR.md).
