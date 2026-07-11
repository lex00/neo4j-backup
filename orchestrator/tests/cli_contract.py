"""Reusable conformance harness for the CLI contract (#60).

Not a test module itself (no `test_` prefix, so pytest doesn't collect it) — the #58 Phase 1 CLI
tests import these helpers and call them per subcommand, so "does this command honour the contract"
is one assertion, not copy-pasted checks. `test_cli_contract.py` exercises the harness against a
stub command here to prove it works before any real subcommand exists.
"""

from __future__ import annotations

from typing import Any, Callable

from neo4j_backup_core.cli_contract import Exit, validate_envelope


def assert_conformant(obj: Any, *, expect_ok: bool | None = None) -> None:
    """The `--json` envelope validates; optionally pin whether it should report success."""
    problems = validate_envelope(obj)
    assert not problems, f"envelope violates the contract: {problems} -- {obj!r}"
    if expect_ok is not None:
        assert obj["ok"] is expect_ok, f"expected ok={expect_ok}, got {obj['ok']}"


def assert_exit(code: int, expected: Exit) -> None:
    """Exit code matches a documented class (not just non-zero)."""
    assert code == expected, f"expected exit {expected!r} ({int(expected)}), got {code}"


def assert_dry_run_is_inert(run: Callable[[list[str]], Any], argv: list[str], *, tripwire) -> Any:
    """A mutating command under `--dry-run` returns cleanly and never fires the side-effect
    tripwire. `tripwire` is any object with a truthy `.called` (e.g. a Mock) or a 0-arg callable
    returning a mutation count; `run` returns `(envelope, exit_code)`."""
    obj, code = run([*argv, "--dry-run"])
    assert_conformant(obj, expect_ok=True)
    assert_exit(code, Exit.OK)
    fired = tripwire.called if hasattr(tripwire, "called") else tripwire()
    assert not fired, "dry-run performed a side effect"
    return obj


def assert_destructive_refused(run: Callable[[list[str]], Any], argv: list[str]) -> Any:
    """A destructive command without `--confirm` refuses: GUARD exit + a conformant error
    envelope. `run` returns `(envelope, exit_code)`."""
    obj, code = run(argv)  # no --confirm
    assert_conformant(obj, expect_ok=False)
    assert_exit(code, Exit.GUARD)
    return obj
