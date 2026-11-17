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
    """MCP.md must document exactly the real tools and prompts (drift guard, like the CLI's doc
    test): each has a table row starting with its name, and every row names a real one."""
    from neo4j_backup_mcp import prompts as prompt_mod

    doc = open(os.path.join(REPO, "MCP.md")).read()
    real = {fn.__name__ for fn in tools.READ_TOOLS + tools.WRITE_TOOLS + prompt_mod.PROMPTS}
    rows = set(re.findall(r"^\| `([a-z_]+)\(", doc, re.M))
    assert real <= rows, f"MCP.md has no row for {sorted(real - rows)}"
    assert rows <= real, f"MCP.md documents non-existent tools or prompts: {sorted(rows - real)}"
    mentioned = set(re.findall(r"\b(?:run|preview)_[a-z_]+", doc))
    assert mentioned <= real, f"MCP.md references non-existent tools: {sorted(mentioned - real)}"


class _Group:
    id = "demo"
    names = ["orders", "customers"]
    retention_days = 7
    restore_mode = "alias-swap"
    tenant = "acme"


class _Policy:
    db_groups = [_Group()]

    def group(self, gid):
        return _Group()

    def select(self, group=None, tenant=None):
        from neo4j_backup_core.policy import SelectionError
        if group and tenant:
            raise SelectionError("both")
        if tenant and tenant != "acme":
            raise SelectionError(f"unknown tenant {tenant!r} (tenants: acme)")
        return [_Group()]

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

    def list_load_artifacts(self, prefix):
        return []

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
    assert tools.list_targets() == {"targets": ["demo/orders", "demo/customers"],
                                    "tenants": {"demo": "acme"}, "clusters": {"demo": "default"}}


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
                        lambda *a, dry_run=False, **k: {"deleted": 2, "detail": {}, "keys": ["a", "b"], "kept_for_chains": 0, "unknown_kept": 0, "loads": [],
                                                        "dry_run": dry_run})
    assert tools.run_prune(dry_run=True)["keys"] == ["a", "b"]
    assert tools.run_prune()["needs_confirmation"] is True
    applied = tools.run_prune(confirm=True)
    assert applied["deleted"] == 2 and "needs_confirmation" not in applied


# --- workflow tools (#273, #274) ---------------------------------------------------------------

def _row(target, state, newest=None, age=None, rpo=60, **kw):
    return {"target": target, "state": state, "newest_point": newest, "age_minutes": age,
            "rpo_minutes": rpo, **kw}


def test_coverage_report_groups_by_state_with_next_steps(monkeypatch):
    rows = [_row("demo/orders", "ok", "2026-11-17T06:00:00+00:00", 5),
            _row("demo/customers", "stale", "2026-11-17T01:00:00+00:00", 300),
            _row("new/db", "stale"),
            _row("logs/ev", "by_design", reason="protect_writes: false"),
            _row("old/shop", "retired", deletes_on="2027-01-01")]
    monkeypatch.setattr(tools, "_status", lambda t, g: {"targets": rows, "stale": [],
                                                       "unmanaged": ["x/"]})
    out = tools.coverage_report()
    assert out["summary"] == {"ok": 1, "stale": 2, "by_design": 1, "retired": 1, "unmanaged": 1}
    stale = {s["target"]: s for s in out["stale"]}
    assert stale["new/db"]["why"] == "never backed up"
    assert stale["new/db"]["next"] == "run_backup(group='new', kind='FULL')"
    assert "300" in stale["demo/customers"]["why"] and "60" in stale["demo/customers"]["why"]
    assert stale["demo/customers"]["next"] == "run_backup(group='demo')"
    assert out["by_design"] == [{"target": "logs/ev", "reason": "protect_writes: false"}]


def _points(monkeypatch):
    pts = {"group": "demo", "members": [
        {"member": "orders", "chains": [{"from": "2026-11-17T06:00:00", "to": "2026-11-17T07:00:00"}]},
        {"member": "customers", "chains": []}]}
    monkeypatch.setattr(tools, "restore_points", lambda group=None, tenant=None: pts)


def test_restore_plan_covered_until_has_a_customer_message(monkeypatch):
    _points(monkeypatch)
    monkeypatch.setattr(tools.ops, "plan_restore", lambda *a, **k: {
        "mode": "alias-swap", "drops": [],
        "members": [{"alias": "orders", "old": "orders-1"}, {"alias": "customers", "old": "c-1"}]})
    out = tools.restore_plan("demo", until="2026-11-17T06:30:00Z", ticket="INC-7")
    assert out["covered"] is True and out["plan"]["members"][0]["old"] == "orders-1"
    msg = out["message"]
    assert "orders, customers (demo)" in msg and "2026-11-17T06:30:00" in msg
    assert "lost" in msg and "retried" in msg and "INC-7" in msg
    assert out["warnings"] == []


def test_restore_plan_keeps_warnings_out_of_the_customer_message(monkeypatch):
    _points(monkeypatch)
    monkeypatch.setattr(tools.ops, "plan_restore", lambda *a, **k: {
        "drops": [], "members": [{"alias": "orders"}],
        "point": {"aligned": False, "problem": "x"}, "alignment_warning": "chain gap in orders"})
    out = tools.restore_plan("demo")
    assert out["warnings"] == ["chain gap in orders"] and "gap" not in out["message"]
    assert "may not be the same moment" in out["message"]


def test_restore_plan_uncovered_until_lists_windows(monkeypatch):
    _points(monkeypatch)

    def refuse(*a, **k):
        raise tools.ops.OpError("no chain covers 2020-01-01")
    monkeypatch.setattr(tools.ops, "plan_restore", refuse)
    out = tools.restore_plan("demo", until="2020-01-01T00:00:00Z")
    assert out["covered"] is False and "no chain covers" in out["error"]
    assert out["windows"]["orders"] == [{"from": "2026-11-17T06:00:00", "to": "2026-11-17T07:00:00"}]
    assert "message" not in out


def test_restore_plan_latest_names_the_common_point(monkeypatch):
    _points(monkeypatch)
    at = datetime(2026, 11, 17, 6, 59, tzinfo=timezone.utc)
    monkeypatch.setattr(tools.ops, "plan_restore", lambda *a, **k: {
        "drops": [], "members": [{"alias": "orders"}, {"alias": "customers"}],
        "point": {"aligned": True, "at": at}})
    out = tools.restore_plan("demo")
    assert "2026-11-17T06:59:00+00:00" in out["message"] and "newest moment" in out["message"]


def test_validate_policy(tmp_path):
    good = os.path.join(REPO, "policies", "demo.yaml")
    assert tools.validate_policy(good) == {"file": good, "valid": True, "errors": []}
    bad = tmp_path / "p.yaml"
    bad.write_text("db_groups: [{id: x}]\n")
    out = tools.validate_policy(str(bad))
    assert out["valid"] is False and out["errors"]
    assert tools.validate_policy("/etc/passwd")["valid"] is False  # only policy files


def test_preview_adopt_is_inert(monkeypatch):
    from neo4j_backup_core import adopt
    monkeypatch.setattr(adopt, "plan_adopt", lambda neo, g, **k: {"members": [], "drops": [], **k})
    monkeypatch.setattr(adopt, "adopt", lambda *a, **k: (_ for _ in ()).throw(AssertionError))
    assert tools.preview_adopt("demo", migrate=True)["plan"]["migrate"] is True


def test_run_export_guards(monkeypatch):
    monkeypatch.setattr(tools.ops, "choose_seeds", lambda *a, **k: {
        "orders": {"key": "k1", "until": None}})
    calls = []
    monkeypatch.setattr(tools, "_run_admin", lambda runner: None)
    monkeypatch.setattr(tools.ops, "export_dumps", lambda *a, **k: calls.append(1) or {"exports": []})
    assert tools.run_export("demo", dry_run=True)["plan"]["members"][0]["from"] == "k1"
    assert tools.run_export("demo")["needs_confirmation"] is True
    assert calls == []
    assert tools.run_export("demo", confirm=True)["exports"] == [] and calls == [1]


def test_backup_status_one_group(monkeypatch):
    out = tools.backup_status(group="demo")
    assert {r["target"] for r in out["targets"]} == {"demo/orders", "demo/customers"}
    assert "unmanaged" not in out  # narrowed: no policy-wide folders


# --- workflow prompts (#272) -------------------------------------------------------------------

from neo4j_backup_mcp import prompts  # noqa: E402

_ARGS = {"onboard": {"group": "g"}, "check_coverage": {}, "restore_request": {"group": "g"},
         "bulk_load": {"group": "g"}, "offboard": {"group": "g"},
         "recover_cluster": {"from_cluster": "east", "to_cluster": "west"}}


@pytest.mark.parametrize("writes", [True, False])
@pytest.mark.parametrize("fn", prompts.PROMPTS, ids=lambda f: f.__name__)
def test_prompts_name_real_tools_and_preview_before_confirm(fn, writes):
    text = fn(**_ARGS[fn.__name__], writes=writes)
    real = {t.__name__ for t in tools.READ_TOOLS + tools.WRITE_TOOLS}
    named = set(re.findall(r"`([a-z_]+)\(", text))
    assert named <= real, f"{fn.__name__} names unknown tools {named - real}"
    if not writes:  # read-only: no run_* tool calls, the CLI instead
        assert not re.search(r"`run_[a-z_]+\(", text), text
    for step in re.findall(r"^\d+\. (.*)$", text, re.M):
        if "confirm=true" in step:  # a change: previewed, then the operator's yes
            first = step.find("dry_run=true")
            assert step.startswith("Call `preview_") or 0 <= first < step.index("confirm=true"), \
                step
            assert "Only after a yes" in step, step
        if "--confirm" in step:
            assert "--dry-run" in step or step.startswith("Call `preview_"), step
