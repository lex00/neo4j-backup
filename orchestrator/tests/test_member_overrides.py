"""#90: per-member storage prefix and physical_base."""

import pytest

from neo4j_backup_core import naming, ops, paths
from neo4j_backup_core import policy as P
from test_artifacts import MemStore
from test_choose_seed import Neo

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def pol(groups):
    return P.Policy.model_validate({"db_groups": groups, "tiers": TIERS})


def test_rename_keeps_history_through_prefix():
    # the alias was `orders`; renamed to `orders-v2`, pinned to the old folder first
    p = pol([{"id": "shop", "tier": "gold", "aliases": ["orders-v2"],
              "overrides": {"orders-v2": {"prefix": "shop/orders/"}}}])
    layout = paths.DefaultPathLayout()
    layout.prefix_for = paths.prefix_from(p)
    s = MemStore()
    s.add("shop/orders/orders-20261101t000000/full.backup")
    assert layout.alias_prefix("shop", "orders-v2") == "shop/orders/"
    neo = Neo()
    out = ops.restore_group(neo, s, p.group("shop"), layout)
    assert out["members"][0]["key"] == "shop/orders/orders-20261101t000000/full.backup"


def test_get_layout_reads_the_policy(monkeypatch, tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text("db_groups:\n- {id: shop, tier: gold, aliases: [a1],\n"
                 "   overrides: {a1: {prefix: legacy/a1/}}}\n"
                 "tiers: {gold: {full_cron: '0 2 * * *', diff_cron: '0 * * * *'}}\n")
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    layout = paths.get_layout()
    assert layout.alias_prefix("shop", "a1") == "legacy/a1/"
    assert layout.physical_prefix("shop", "a1", "p") == "legacy/a1/p/"
    assert layout.load_prefix("shop", "a1") == "legacy/a1/_seed/"
    assert layout.alias_prefix("other", "xyz") == "other/xyz/"


@pytest.mark.parametrize("groups,why", [
    ([{"id": "a", "tier": "gold", "aliases": ["x", "y"],
       "overrides": {"x": {"prefix": "same/"}, "y": {"prefix": "same/"}}}], "share"),
    ([{"id": "a", "tier": "gold", "aliases": ["x", "y"],
       "overrides": {"x": {"prefix": "data/"}, "y": {"prefix": "data/y/"}}}], "nest"),
    ([{"id": "a", "tier": "gold", "aliases": ["xyz"]},
      {"id": "b", "tier": "gold", "aliases": ["y"], "overrides": {"y": {"prefix": "a/xyz/"}}}],
     "share"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"], "overrides": {"x": {"prefix": "_state/x/"}}}],
     "reserved"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"], "overrides": {"x": {"prefix": "a/_seed/"}}}],
     "reserved"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"], "overrides": {"x": {"prefix": "no-slash"}}}],
     "ending in '/'"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"], "overrides": {"z": {"prefix": "z/"}}}],
     "not a member"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"],
       "overrides": {"x": {"physical_base": "Bad_Base"}}}], "physical_base"),
    ([{"id": "a", "tier": "gold", "aliases": ["x"],
       "overrides": {"x": {"physical_base": "b" * 50}}}], "room for the timestamp"),
])
def test_rejected_overrides(groups, why):
    with pytest.raises(Exception, match=why):
        pol(groups)


def test_physical_base_names_and_owns():
    g = pol([{"id": "shop", "tier": "gold", "aliases": ["Orders"],
              "overrides": {"Orders": {"physical_base": "orders"}}}]).group("shop")
    ts = "20261116t120000"
    name = naming.for_group(g).physical("Orders", ts)
    assert name == f"orders-{ts}" and naming.for_group(g).owns("Orders", name)
    assert not naming.for_group(g).owns("Orders", f"orders-cded0933-{ts}")
