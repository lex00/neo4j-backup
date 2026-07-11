---
name: neo4j-backup
description: Drive Neo4j Enterprise backup and restore in this repo via the neo4j-backup CLI — back up, verify, restore (including point-in-time), aggregate, prune, and export/replay DBMS metadata, with safe --dry-run/--confirm guardrails. Use when an operator asks to back up, check, restore, or clean up Neo4j databases here.
---

# neo4j-backup

Use the **`neo4j-backup` CLI** to operate backups in this repo. The full guide, including the safety
rules you must follow, is [AGENTS.md](../../../AGENTS.md); the machine contract is
[CLI-CONTRACT.md](../../../CLI-CONTRACT.md). This skill is a pointer — do not restate the contract,
read those.

Core rules:

- Always pass `--json` and parse the result envelope (`ok`, `op`, `group`, `result`, `error`).
- Read-only by default: `targets`, `verify`, and any `--dry-run` are safe to run.
- The mutating commands (`restore`, `prune`, `aggregate`, `metadata restore`) require `--confirm`
  and exit `3` without it. Never pass `--confirm` until a human approves the specific action, and
  always run `--dry-run` first and show them the blast radius it reports.

Start by running `neo4j-backup --json targets` to see what is configured, then read AGENTS.md for the
command surface and worked examples.
