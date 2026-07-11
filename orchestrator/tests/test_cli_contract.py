"""#60 — the CLI contract is executable before any CLI exists: validate the envelope primitives
and drive the conformance harness against a stub command that models the contract's rules."""

from unittest.mock import Mock

import pytest

from neo4j_backup_core.cli_contract import Exit, envelope, make_error, validate_envelope

from cli_contract import (
    assert_conformant,
    assert_destructive_refused,
    assert_dry_run_is_inert,
    assert_exit,
)


# --- the primitives -------------------------------------------------------------------------

def test_success_envelope_is_conformant():
    obj = envelope("backup", ok=True, group="demo", result={"key": "demo/orders/.../full.backup"})
    assert validate_envelope(obj) == []
    assert obj["error"] is None


def test_failure_envelope_is_conformant():
    obj = envelope("restore", ok=False, group="demo", error=make_error("server_refused", "boom"))
    assert validate_envelope(obj) == []


@pytest.mark.parametrize("obj, needle", [
    ({"ok": True, "op": "x"}, "missing keys"),                                  # truncated
    (envelope("x", ok=True) | {"stray": 1}, "unexpected keys"),
    (envelope("", ok=True), "non-empty string"),                               # empty op
    (envelope("x", ok=True, error=make_error("k", "m")), "must be null"),      # ok+error
    (envelope("x", ok=False), "must be an object"),                            # failure sans error
    ({"ok": "yes", "op": "x", "group": None, "result": None, "error": None}, "`ok` must be a bool"),
    (envelope("x", ok=True, group=7), "must be a string or null"),
])
def test_validate_rejects_malformed(obj, needle):
    problems = validate_envelope(obj)
    assert any(needle in p for p in problems), problems


def test_exit_codes_are_stable():
    assert (Exit.OK, Exit.FAILURE, Exit.USAGE, Exit.GUARD) == (0, 1, 2, 3)


# --- a stub command that honours the contract, driven through the harness -------------------

def make_stub():
    """A minimal destructive 'prune'-like command: mutates via `sink` unless --dry-run, and
    refuses without --confirm. Returns `(run, sink)` where run(argv) -> (envelope, exit_code)."""
    sink = Mock()

    def run(argv):
        dry = "--dry-run" in argv
        confirmed = "--confirm" in argv
        if not confirmed and not dry:
            return (
                envelope("prune", ok=False, group="demo",
                         error=make_error("confirm_required", "pass --confirm to delete")),
                Exit.GUARD,
            )
        if not dry:
            sink()  # the side effect
        return envelope("prune", ok=True, group="demo",
                        result={"blast_radius": {"keys_to_delete": ["demo/orders/old/full.backup"]},
                                "applied": not dry}), Exit.OK

    return run, sink


def test_stub_dry_run_is_inert():
    run, sink = make_stub()
    obj = assert_dry_run_is_inert(run, ["prune", "--confirm"], tripwire=sink)
    assert obj["result"]["applied"] is False
    assert "blast_radius" in obj["result"]  # destructive op echoes what it would remove


def test_stub_refuses_without_confirm():
    run, _ = make_stub()
    obj = assert_destructive_refused(run, ["prune"])
    assert obj["error"]["kind"] == "confirm_required"


def test_stub_applies_with_confirm():
    run, sink = make_stub()
    obj, code = run(["prune", "--confirm"])
    assert_conformant(obj, expect_ok=True)
    assert_exit(code, Exit.OK)
    sink.assert_called_once()
    assert obj["result"]["applied"] is True
