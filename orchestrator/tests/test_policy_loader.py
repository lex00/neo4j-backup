"""#46 POLICY_LOADER override — a team-supplied fetcher for authenticated/custom delivery.

These tests inject a real module into sys.modules and let the production importlib resolution
run — nothing in the resolution path is patched (that's the point of the seam)."""

import sys
import types

import pytest

from neo4j_backup_core import policy as P

MINI = """
db_groups:
  - id: g
    aliases: [a]
    tier: gold
    s3_prefix: s3://x/
tiers:
  gold: { full_cron: "0 2 * * *", diff_cron: "0 * * * *" }
"""


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    P._cache.clear()
    monkeypatch.setenv("POLICY_CACHE_TTL", "0")  # don't let the cache mask fetch behaviour
    yield
    P._cache.clear()


def _install_loader(monkeypatch, fn, name="fake_policy_loader"):
    """Register a real importable module named `name` exposing `fetch = fn`; point POLICY_LOADER
    at it. Both are reverted by the fixture."""
    mod = types.ModuleType(name)
    mod.fetch = fn
    monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setenv("POLICY_LOADER", f"{name}.fetch")


def test_custom_loader_is_used_with_source(monkeypatch):
    seen = {}

    def fetch(source):
        seen["source"] = source
        return MINI

    _install_loader(monkeypatch, fetch)
    pol = P.load_policy("https://config.internal/neo4j/policy")   # loader's own scheme
    assert seen["source"] == "https://config.internal/neo4j/policy"
    assert pol.group("g").aliases == ["a"]


def test_bad_spec_raises(monkeypatch):
    monkeypatch.setenv("POLICY_LOADER", "NoDotHere")
    with pytest.raises(RuntimeError, match="module.callable"):
        P.load_policy("whatever")


def test_loader_failure_falls_back_to_last_known_good(monkeypatch):
    state = {"fail": False}

    def fetch(source):
        if state["fail"]:
            raise RuntimeError("401 from config endpoint")
        return MINI

    _install_loader(monkeypatch, fetch)
    good = P.load_policy("x")            # caches last-known-good
    state["fail"] = True
    assert P.load_policy("x", force=True) is good   # endpoint down -> serve last good


def test_unset_falls_back_to_file(monkeypatch, tmp_path):
    monkeypatch.delenv("POLICY_LOADER", raising=False)
    f = tmp_path / "p.yaml"
    f.write_text(MINI)
    assert P.load_policy(str(f)).group("g")   # built-in file path still works


# --- #73: full interval vs retention ------------------------------------------------------------

def _policy(tier_full, retention):
    from neo4j_backup_core.policy import Policy
    return Policy.model_validate({
        "db_groups": [{"id": "g", "aliases": ["a"], "tier": "t", "s3_prefix": "s3://x/",
                       "retention_days": retention}],
        "tiers": {"t": {"full_cron": tier_full, "diff_cron": "0 * * * *"}}})


def test_warns_when_fulls_are_further_apart_than_retention(caplog):
    import logging
    with caplog.at_level(logging.WARNING):
        p = _policy("0 3 * * 0", 3)  # weekly full, 3 days retention
    assert len(p.warnings()) == 1 and "7 days" in p.warnings()[0]
    assert "retention_days 3" in caplog.text


def test_no_warning_when_retention_covers_the_full_interval():
    assert _policy("0 3 * * 0", 7).warnings() == []
    assert _policy("0 2 * * *", 1).warnings() == []


def test_cron_forms_it_cannot_read_are_skipped():
    # Dagster and Airflow accept @weekly and day names; the check skips them, load still works
    assert _policy("@weekly", 3).warnings() == []
    assert _policy("0 3 * * SUN", 3).warnings() == []


def test_shipped_demo_policy_has_no_warnings():
    from neo4j_backup_core.policy import load_policy
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    assert load_policy(os.path.join(here, "..", "..", "policies", "demo.yaml")).warnings() == []
