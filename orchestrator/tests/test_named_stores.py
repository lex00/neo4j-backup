"""#105: named stores; every op resolves its store from the group."""

import logging

import pytest

from neo4j_backup_core import env, ops, paths
from neo4j_backup_core.policy import Policy, StoreConfig
from test_artifacts import MemStore

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def pol(**extra):
    return Policy.model_validate({
        "storage": {"finance": {"url": "s3://neo4j-backups-finance",
                                "sse_kms_key_id": "alias/finance"}},
        "db_groups": [{"id": "billing", "aliases": ["billing"], "tier": "gold",
                       "storage": "finance"},
                      {"id": "shop", "aliases": ["shop"], "tier": "gold"}],
        "tiers": TIERS, **extra})


def test_store_names_resolve_with_default():
    p = pol()
    assert p.store_name("billing") == "finance" and p.store_name("shop") == "default"


def test_unknown_store_names_the_group():
    with pytest.raises(ValueError, match="group 'x': storage 'nope' is not a store"):
        Policy.model_validate({"db_groups": [{"id": "x", "aliases": ["a"], "tier": "gold",
                                              "storage": "nope"}], "tiers": TIERS})


def test_unknown_load_source_store_is_rejected():
    with pytest.raises(ValueError, match="load.source.storage 'nope'"):
        Policy.model_validate({"db_groups": [{
            "id": "x", "aliases": ["a"], "tier": "gold",
            "load": {"mode": "once", "source": {"storage": "nope"}}}], "tiers": TIERS})


@pytest.mark.parametrize("url", ["http://x", "s3://", "s3://b/prefix", "azb://onlyaccount"])
def test_bad_store_urls(url):
    with pytest.raises(ValueError):
        StoreConfig(url=url)


def test_store_url_parts():
    assert (StoreConfig(url="azb://acct/cont").cloud, StoreConfig(url="azb://acct/cont").bucket,
            StoreConfig(url="azb://acct/cont").account) == ("azure", "cont", "acct")
    assert StoreConfig(url="gs://b").cloud == "gcp"


def test_s3_prefix_is_deprecated(caplog):
    with caplog.at_level(logging.WARNING):
        Policy.model_validate({"db_groups": [{"id": "x", "aliases": ["a"], "tier": "gold",
                                              "s3_prefix": "s3://old/"}], "tiers": TIERS})
    assert "s3_prefix" in caplog.text


def test_storeset_routes_each_group(monkeypatch):
    default, finance = MemStore(), MemStore()
    monkeypatch.setattr(env, "store_from_config",
                        lambda cfg: finance if cfg.bucket == "neo4j-backups-finance" else None)
    s = env.StoreSet(default, policy=pol())
    assert s.for_group("billing") is finance and s.for_group("shop") is default
    s.put_text("k", "v")  # protocol methods go to the default store
    assert default.objects["k"] == "v"


def test_no_storage_block_keeps_the_environment_store():
    default = MemStore()
    p = Policy.model_validate({"db_groups": [{"id": "x", "aliases": ["a"], "tier": "gold"}],
                               "tiers": TIERS})
    assert env.StoreSet(default, policy=p).for_group("x") is default


def test_ops_back_up_into_the_groups_store(monkeypatch):
    default, finance = MemStore(), MemStore()
    monkeypatch.setattr(env, "store_from_config", lambda cfg: finance)
    s = env.StoreSet(default, policy=pol())

    class Neo:
        def resolve_physical(self, a):
            return "billing-1"

    calls = []

    def run(cmd):
        calls.append(cmd)
        if "backup" in cmd and "inspect" not in cmd:
            finance.add("billing/billing/billing-1/x.backup")
        return "neo4j 2026.05.0\n[]"
    from neo4j_backup_core.clients import BackupRunner
    out = ops.backup_target(run, Neo(), s, BackupRunner(), paths.DefaultPathLayout(), "billing",
                            "billing", "FULL")
    assert out["artifact"] == "billing/billing/billing-1/x.backup"
    assert finance.objects and not default.objects
    assert any("s3://bucket/billing/billing/billing-1/" in " ".join(c) for c in calls)


def test_prune_sweeps_each_groups_store(monkeypatch):
    from datetime import datetime, timezone
    default, finance = MemStore(), MemStore()
    finance.add("billing/billing/p/x.backup")
    monkeypatch.setattr(env, "store_from_config", lambda cfg: finance)
    s = env.StoreSet(default, policy=pol())
    seen = []
    orig = ops.artifact_chains

    def spy(store, *a, **k):
        seen.append(store)
        return orig(store, *a, **k)
    monkeypatch.setattr(ops, "artifact_chains", spy)
    ops.prune(s, pol(), paths.DefaultPathLayout(), dry_run=True,
              now=datetime(2026, 11, 15, tzinfo=timezone.utc))
    assert finance in seen
