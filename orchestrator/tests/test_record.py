"""#119: --ticket / --reason on mutating commands, recorded with the result."""

import pytest

from neo4j_backup_core import record
from neo4j_backup_core.cli_contract import Exit
from test_cli import assert_conformant, assert_exit, run  # the CLI harness


def test_fields_trim_and_default():
    assert record.fields(" CHG-1 ", None, applying=True) == {"ticket": "CHG-1", "reason": None}


@pytest.mark.parametrize("bad", ["two\nlines", "x" * 501])
def test_fields_reject_multiline_or_long(bad):
    with pytest.raises(record.TicketError):
        record.fields(bad, None, applying=False)


def test_required_only_when_applying(monkeypatch):
    monkeypatch.setenv("NEO4J_BACKUP_REQUIRE_TICKET", "true")
    assert record.fields(None, None, applying=False)["ticket"] is None  # dry run is fine
    with pytest.raises(record.TicketError):
        record.fields(None, "why", applying=True)


def _prune(monkeypatch):
    import neo4j_backup_cli.__main__ as cli
    monkeypatch.setattr(cli.ops, "prune", lambda *a, dry_run=False, **k: {
        "deleted": 0, "detail": {}, "keys": [], "kept_for_chains": 0, "unknown_kept": 0,
        "dry_run": dry_run})


def test_cli_records_ticket_and_reason(monkeypatch):
    _prune(monkeypatch)
    env, code = run(["prune", "--confirm", "--ticket", "CHG-42", "--reason", "quarterly cleanup"])
    assert_conformant(env, expect_ok=True)
    assert env["result"]["ticket"] == "CHG-42" and env["result"]["reason"] == "quarterly cleanup"


def test_cli_requires_a_ticket_when_configured(monkeypatch):
    _prune(monkeypatch)
    monkeypatch.setenv("NEO4J_BACKUP_REQUIRE_TICKET", "1")
    env, code = run(["prune", "--confirm"])
    assert_conformant(env, expect_ok=False)
    assert_exit(code, Exit.GUARD) and env["error"]["kind"] == "ticket_required"
    env, code = run(["prune", "--dry-run"])  # previews don't need one
    assert_exit(code, Exit.OK)


def test_mcp_write_tools_record_and_require(monkeypatch):
    from neo4j_backup_mcp import tools
    monkeypatch.setattr(tools.ops, "prune", lambda *a, dry_run=False, **k: {
        "deleted": 1, "detail": {}, "keys": ["a"], "kept_for_chains": 0, "unknown_kept": 0,
        "dry_run": dry_run})
    monkeypatch.setattr(tools.env, "runner", lambda: None)
    monkeypatch.setattr(tools, "_run_admin", lambda r: None)
    out = tools.run_prune(confirm=True, ticket="CHG-7", reason="r")
    assert out["ticket"] == "CHG-7" and out["deleted"] == 1
    monkeypatch.setenv("NEO4J_BACKUP_REQUIRE_TICKET", "true")
    with pytest.raises(record.TicketError):
        tools.run_prune(confirm=True)
