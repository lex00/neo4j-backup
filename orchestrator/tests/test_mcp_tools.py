"""#58 P5 — the MCP tool logic (`neo4j_backup_mcp.tools`), exercised with stubs and no `mcp`
dependency (so it runs in CI without the SDK). Pins the guardrails: read tools are inert, mutating
tools refuse without confirm, dry-run previews the blast radius, and destructive restore runs
verify-before-drop."""

import os
import re
from datetime import datetime, timezone

import pytest

from neo4j_backup_mcp import tools

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_mcp_doc_covers_every_tool_and_no_phantoms():
    """MCP.md must document exactly the real tool set (drift guard, like the CLI's doc test)."""
    doc = open(os.path.join(REPO, "MCP.md")).read()
    real = {fn.__name__ for fn in tools.READ_TOOLS + tools.WRITE_TOOLS}
    for name in real:
        assert name in doc, f"MCP.md does not document tool {name}"
    mentioned = set(re.findall(r"\b(?:run|preview)_[a-z_]+", doc))
    assert mentioned <= real, f"MCP.md references non-existent tools: {sorted(mentioned - real)}"


class _Group:
    id = "demo"
    names = ["orders", "customers"]
    retention_days = 7
    restore_mode = "alias-swap"


class _Policy:
    db_groups = [_Group()]

    def group(self, gid):
        return _Group()

    def partition_keys(self):
        return ["demo/orders", "demo/customers"]


class _Store:
    def __init__(self, arts=None):
        self._arts = arts or {}

    def latest_artifact_key(self, prefix):
        a = self._arts.get(prefix)
        return max(a, key=lambda t: t[2])[0] if a else None

    def list_artifacts(self, prefix):
        return self._arts.get(prefix, [])

    def object_size(self, key):
        return 123


@pytest.fixture(autouse=True)
def stub_env(monkeypatch):
    monkeypatch.setattr(tools.env, "store", lambda: _Store())
    monkeypatch.setattr(tools.env, "neo4j", lambda: object())
    monkeypatch.setattr(tools.env, "runner", lambda: object())
    monkeypatch.setattr(tools.env, "policy_path", lambda: "x.yaml")
    monkeypatch.setattr(tools, "load_policy", lambda p: _Policy())


# --- read-only --------------------------------------------------------------------------------

def test_list_targets():
    assert tools.list_targets() == {"targets": ["demo/orders", "demo/customers"]}


def test_backup_status_reports_age(monkeypatch):
    from neo4j_backup_core import paths
    prefix = paths.DefaultPathLayout().alias_prefix("demo", "orders")
    old = datetime(2020, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(tools.env, "store", lambda: _Store({prefix: [(f"{prefix}f.backup", 1, old)]}))
    out = tools.backup_status()
    row = next(r for r in out["targets"] if r["target"] == "demo/orders")
    assert row["latest"].endswith("f.backup") and row["age_hours"] > 0
    missing = next(r for r in out["targets"] if r["target"] == "demo/customers")
    assert missing["latest"] is None  # never backed up


def test_preview_restore_is_inert(monkeypatch):
    monkeypatch.setattr(tools.ops, "plan_restore",
                        lambda *a, **k: {"mode": "by-name", "drops": ["orders"], "members": []})
    out = tools.preview_restore("demo", replace=True)
    assert out["plan"]["drops"] == ["orders"]


# --- mutating guards --------------------------------------------------------------------------

def test_run_restore_dry_run_does_not_mutate(monkeypatch):
    monkeypatch.setattr(tools.ops, "plan_restore", lambda *a, **k: {"drops": [], "members": []})
    monkeypatch.setattr(tools.ops, "restore_group",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("mutated on dry-run")))
    out = tools.run_restore("demo", dry_run=True)
    assert out["dry_run"] is True and "plan" in out


def test_run_restore_without_confirm_needs_confirmation(monkeypatch):
    monkeypatch.setattr(tools.ops, "plan_restore", lambda *a, **k: {"drops": [], "members": []})
    out = tools.run_restore("demo")
    assert out["needs_confirmation"] is True and out["ok"] is False


def test_run_restore_replace_verifies_before_drop(monkeypatch):
    order = []
    monkeypatch.setattr(tools.ops, "plan_restore",
                        lambda *a, **k: {"drops": ["orders"], "members": []})
    monkeypatch.setattr(tools, "_run_admin", lambda runner: (lambda cmd: None))
    monkeypatch.setattr(tools.ops, "verify_target",
                        lambda *a, **k: order.append("verify") or {"consistent": True})
    monkeypatch.setattr(tools.ops, "restore_group",
                        lambda *a, **k: order.append("restore") or {"members": []})
    out = tools.run_restore("demo", replace=True, confirm=True)
    assert out["verified_before_drop"] is True
    assert order[0] == "verify" and order[-1] == "restore"  # verified before any drop/restore


def test_run_prune_guards(monkeypatch):
    monkeypatch.setattr(tools.ops, "prune",
                        lambda *a, dry_run=False, **k: {"deleted": 2, "detail": {}, "keys": ["a", "b"],
                                                        "dry_run": dry_run})
    assert tools.run_prune(dry_run=True)["keys"] == ["a", "b"]
    assert tools.run_prune()["needs_confirmation"] is True
    applied = tools.run_prune(confirm=True)
    assert applied["deleted"] == 2 and "needs_confirmation" not in applied
