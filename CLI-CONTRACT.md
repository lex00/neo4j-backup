# CLI contract

The `neo4j-backup` CLI is the interface a **coding/ops agent drives without MCP** (and the surface
CI schedules). For that to be safe and scriptable, every subcommand honours one contract: a stable
machine-readable result, documented exit codes, and explicit guards on anything destructive. The
optional MCP server ([#58](https://github.com/lex00/neo4j-backup/issues/58) Phase 5) is a thin
wrapper over this same contract, not a separate interface.

The contract is executable: [`neo4j_backup_core/cli_contract.py`](orchestrator/neo4j_backup_core/cli_contract.py)
is the single source of truth, and [`tests/cli_contract.py`](orchestrator/tests/cli_contract.py) is
the conformance harness the CLI's tests call per subcommand. This document is its prose form.

## Output: `--json` on every command

Human-readable output is the default. `--json` switches stdout to a single JSON object — the
**envelope** — so an agent or CI parses one thing:

```json
{
  "ok": true,
  "op": "backup",
  "group": "demo",
  "result": { "key": "demo/orders/orders-20260711t120000/full.backup" },
  "error": null
}
```

| Field    | Type                          | Notes |
|----------|-------------------------------|-------|
| `ok`     | bool                          | Always present. |
| `op`     | string                        | The operation, e.g. `backup`, `restore`, `verify`, `prune`. |
| `group`  | string or null                | The policy group acted on, when the command targets one. |
| `result` | object or null                | Operation payload on success (keys, counts, blast radius). |
| `error`  | `{ "kind", "msg" }` or null   | Present **iff** `ok` is false. `kind` is a stable token; `msg` is human text. |

Rules: `error` is an object when `ok` is false and null otherwise; `group`/`result` are the value or
null. **Log lines go to stderr** so stdout stays clean, parseable JSON. `neo4j_backup_core.cli_contract.validate_envelope`
enforces this and returns `[]` for a conformant object.

## Exit codes

Documented classes (`neo4j_backup_core.cli_contract.Exit`), stable across commands, so CI and agents
gate on meaning rather than a bare non-zero:

| Code | Name      | When |
|------|-----------|------|
| `0`  | `OK`      | The operation succeeded. |
| `1`  | `FAILURE` | The operation ran and failed (backup errored, server refused a restore). |
| `2`  | `USAGE`   | Bad arguments / unknown command (argparse). |
| `3`  | `GUARD`   | A safety guard refused: a destructive op without `--confirm`, or a failed precondition. |

## Safety: dry-run, blast radius, confirm

- **`--dry-run`** on every mutating command: print the planned actions, exit `0`, touch neither
  Neo4j nor the object store.
- **Blast radius** for destructive ops (`restore --replace`, `prune`): before acting — and always
  under `--dry-run` — the `result` echoes what will be dropped/created (physical names, alias swaps,
  artifact keys). The MCP layer surfaces this same structure.
- **`--confirm` (or `--yes`) required for destructive ops. No interactive prompts, ever** — an
  agent/CI can't answer one, so a missing confirmation is a `GUARD` refusal, not a hang. Read-only
  is the default posture.

## For implementers

Build each subcommand on `envelope(...)` / `make_error(...)` and return an `Exit` code; do not
hand-roll result dicts. In tests, call the harness (`assert_conformant`, `assert_exit`,
`assert_dry_run_is_inert`, `assert_destructive_refused`) rather than re-checking fields. See
[#60](https://github.com/lex00/neo4j-backup/issues/60) for scope and
[#59](https://github.com/lex00/neo4j-backup/issues/59) for the agent-facing `AGENTS.md` that
summarises this contract.
