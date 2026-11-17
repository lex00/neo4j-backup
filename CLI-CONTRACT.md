# CLI contract

Every `neo4j-backup` subcommand follows one contract, so CI and agents can script it.
It gives a JSON result, fixed exit codes and guards on anything destructive.
The [MCP server](MCP.md) returns the same results.

The source of truth is [`neo4j_backup_core/cli_contract.py`](orchestrator/neo4j_backup_core/cli_contract.py).
The CLI's tests check each subcommand with [`tests/cli_contract.py`](orchestrator/tests/cli_contract.py).

## Output: `--json`

Output is human-readable by default. With `--json`, stdout is one JSON object, the envelope:

```json
{
  "ok": true,
  "op": "backup",
  "group": "demo",
  "result": { "key": "demo/orders/orders-20260711t120000/orders-20260711t120000-2026-07-11T12-00-05.backup" },
  "error": null
}
```

| Field | Type | Notes |
|---|---|---|
| `ok` | bool | Always present. |
| `op` | string | The operation, such as `backup`, `restore`, `verify` or `prune`. |
| `group` | string or null | The policy group, when the command targets one. |
| `result` | object or null | The payload on success: keys, counts, blast radius. |
| `error` | `{ "kind", "msg" }` or null | Set only when `ok` is false. `kind` is a stable token. |

Logs go to stderr, so stdout stays parseable.

## Exit codes

| Code | Name | When |
|---|---|---|
| `0` | `OK` | The operation succeeded. |
| `1` | `FAILURE` | The operation ran and failed, for example a refused restore. |
| `2` | `USAGE` | Bad arguments or an unknown command. |
| `3` | `GUARD` | A guard refused: a destructive op lacks `--confirm`, or a precondition failed. |

## Guards

- `--dry-run` on every mutating command prints the plan and exits `0`. It reads Neo4j and the store and leaves both unchanged.
- Destructive ops (`restore --replace`, `prune`) list in `result` what they drop, create, swap or delete.
- Destructive ops need `--confirm` and skip interactive prompts. A run lacking it is a `GUARD` refusal.
- `--ticket` and `--reason` (one line each, up to 500 characters) go to stderr and to `result.ticket` and `result.reason`.
  They also appear in dry-run and refusal envelopes.
- With `NEO4J_BACKUP_REQUIRE_TICKET=true`, an applied change needs `--ticket` or is refused (`ticket_required`).
  `--dry-run` runs ticket-free.

## Error kinds

Every failure, even an unexpected exception, is an envelope with `ok: false`.

| `error.kind` | Meaning |
|---|---|
| `op_failed` | The operation was refused or failed. |
| `admin_failed` | `neo4j-admin` exited non-zero. |
| `seed_failed` | A group restore failed while seeding. `result.dropped` lists the members dropped again. |
| `baseline_failed` | The restore applied but the follow-up full backup failed. |
| `locked` | Other work held the group's lock for `NEO4J_BACKUP_LOCK_WAIT` seconds. |
| `neo4j_error` | The Neo4j driver raised. |
| `internal_error` | Anything else. |

The traceback goes to stderr only with `--debug` or `NEO4J_BACKUP_DEBUG=1`.

## For implementers

Build each subcommand on `envelope(...)` and `make_error(...)`, and return an `Exit` code.
In tests, use `assert_conformant`, `assert_exit`, `assert_dry_run_is_inert` and `assert_destructive_refused`.
