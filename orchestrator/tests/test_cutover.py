"""#17 pluggable cutover — no live cluster."""

import json
import subprocess
import urllib.request

import pytest

from neo4j_backup_core.cutover import (
    AliasSwapCutover,
    CutoverStrategy,
    ExternalRoutingCutover,
    from_env,
)


class _FakeNeo:
    def __init__(self):
        self.calls = []

    def alter_alias(self, alias, target):
        self.calls.append((alias, target))


def test_alias_swap_is_default_behaviour():
    neo = _FakeNeo()
    AliasSwapCutover().cutover(neo, "orders", "orders-t1", "orders-t0")
    assert neo.calls == [("orders", "orders-t1")]
    assert isinstance(AliasSwapCutover(), CutoverStrategy)  # runtime_checkable


def test_external_command_hook_runs_and_leaves_alias_untouched(monkeypatch):
    captured = {}

    def fake_run(args, **kw):
        captured["args"] = args
        captured["env"] = kw.get("env")

    monkeypatch.setattr(subprocess, "run", fake_run)
    neo = _FakeNeo()
    ExternalRoutingCutover("router-tool --repoint").cutover(neo, "orders", "orders-t1", "orders-t0")

    assert neo.calls == []  # the Neo4j alias is NOT swapped
    assert captured["args"] == ["router-tool", "--repoint"]
    assert captured["env"]["CUTOVER_NEW_PHYSICAL"] == "orders-t1"
    assert captured["env"]["CUTOVER_OLD_PHYSICAL"] == "orders-t0"


def test_external_http_hook_posts_payload(monkeypatch):
    seen = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b""

    def fake_urlopen(req):
        seen["url"] = req.full_url
        seen["data"] = req.data
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    ExternalRoutingCutover("https://router/repoint").cutover(_FakeNeo(), "o", "o-t1", "o-t0")

    assert seen["url"] == "https://router/repoint"
    assert json.loads(seen["data"]) == {"alias": "o", "new_physical": "o-t1", "old_physical": "o-t0"}


def test_from_env_selection(monkeypatch):
    monkeypatch.delenv("CUTOVER_STRATEGY", raising=False)
    assert isinstance(from_env(), AliasSwapCutover)

    monkeypatch.setenv("CUTOVER_STRATEGY", "external")
    monkeypatch.setenv("CUTOVER_HOOK", "some-hook")
    assert isinstance(from_env(), ExternalRoutingCutover)

    monkeypatch.delenv("CUTOVER_HOOK", raising=False)
    with pytest.raises(RuntimeError, match="CUTOVER_HOOK"):
        from_env()

    monkeypatch.setenv("CUTOVER_STRATEGY", "bogus")
    with pytest.raises(RuntimeError, match="unknown CUTOVER_STRATEGY"):
        from_env()
