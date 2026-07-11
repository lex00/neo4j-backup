"""The agent-drivable CLI contract (#60): the shape every `neo4j-backup` subcommand emits.

This is the interface a no-MCP agent (and CI) drives, and the exact surface the optional MCP
server (#58 P5) later wraps — so it lives in framework-free core as one source of truth, importable
by the CLI, the MCP tools, and the conformance harness alike. `CLI-CONTRACT.md` is the prose spec;
this module is its executable form.

An emitted result is a single JSON object on stdout under `--json`:

    {"ok": bool, "op": str, "group": str|null, "result": object|null,
     "error": null | {"kind": str, "msg": str}}

Invariants (enforced by `validate_envelope`): `ok`/`op` always present; `error` is an object iff
`ok` is false and null otherwise; `group`/`result` are the value or null. Human-readable log lines
go to stderr so stdout stays parseable.
"""

from __future__ import annotations

import enum
from typing import Any


class Exit(enum.IntEnum):
    """Documented exit-code classes — stable across commands, for CI *and* agent gating."""

    OK = 0
    FAILURE = 1       # the operation ran and failed (backup errored, restore refused by server)
    USAGE = 2         # bad arguments / unknown command — argparse territory
    GUARD = 3         # a safety guard refused: destructive op without --confirm, failed precondition


def make_error(kind: str, msg: str) -> dict[str, str]:
    """The `error` sub-object. `kind` is a stable machine token; `msg` is human text."""
    return {"kind": kind, "msg": msg}


def envelope(
    op: str,
    *,
    ok: bool,
    group: str | None = None,
    result: Any | None = None,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the canonical result object. On failure pass `ok=False` and an `error` from
    `make_error`; on success leave `error` as None. Mismatches are caught by `validate_envelope`."""
    return {"ok": ok, "op": op, "group": group, "result": result, "error": error}


def validate_envelope(obj: Any) -> list[str]:
    """Return a list of contract violations (empty ⇒ conformant). Hand-rolled to keep core
    dependency-free — no jsonschema. The CLI's tests assert this returns `[]` for every command."""
    problems: list[str] = []
    if not isinstance(obj, dict):
        return ["envelope is not a JSON object"]

    required = {"ok", "op", "group", "result", "error"}
    missing = required - obj.keys()
    if missing:
        problems.append(f"missing keys: {sorted(missing)}")
    extra = obj.keys() - required
    if extra:
        problems.append(f"unexpected keys: {sorted(extra)}")

    ok = obj.get("ok")
    if not isinstance(ok, bool):
        problems.append("`ok` must be a bool")
    if "op" in obj and (not isinstance(obj["op"], str) or not obj["op"]):
        problems.append("`op` must be a non-empty string")
    if obj.get("group") is not None and not isinstance(obj["group"], str):
        problems.append("`group` must be a string or null")

    err = obj.get("error")
    if ok is True and err is not None:
        problems.append("`error` must be null when ok is true")
    if ok is False:
        if not isinstance(err, dict):
            problems.append("`error` must be an object when ok is false")
        elif not (isinstance(err.get("kind"), str) and isinstance(err.get("msg"), str)):
            problems.append("`error` needs string `kind` and `msg`")
    return problems
