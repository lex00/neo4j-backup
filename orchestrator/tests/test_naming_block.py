"""#88: the policy's naming: block (physical_template, ts_format)."""

from datetime import datetime, timezone

import pytest

from neo4j_backup_core import naming, ops, paths
from neo4j_backup_core import policy as P
from test_artifacts import MemStore

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}
NOW = datetime(2026, 11, 16, 12, 30, 5, tzinfo=timezone.utc)


def pol(**kw):
    raw = {"db_groups": [{"id": "shop", "tier": "gold", "aliases": ["Orders", "graph"]}],
           "tiers": TIERS, **kw}
    return P.Policy.model_validate(raw)


def test_defaults_reproduce_todays_names():
    g = pol().group("shop")
    ts = naming.ts_for(g, NOW)
    assert ts == "20261116t123005"
    assert naming.for_group(g).physical("Orders", ts) == naming.physical("Orders", ts)
    g = pol(naming={}).group("shop")  # an empty block is the default too
    assert naming.for_group(g).physical("graph", ts) == f"graph-{ts}"


@pytest.mark.parametrize("template,expect", [
    ("{slug}-{ts}", "graph-20261116t1230"),
    ("{alias}-{ts}", "graph-20261116t1230"),
    ("prod-{slug}-{ts}", "prod-graph-20261116t1230"),
    ("{group}-{alias}-{ts}", "shop-graph-20261116t1230"),
    ("x{ts}-{slug}", "x20261116t1230-graph"),
    ("{ts}-{slug}", "20261116t1230-graph"),  # a digit first is legal (#85)
])
def test_each_field(template, expect):
    g = pol(naming={"physical_template": template, "ts_format": "%Y%m%dt%H%M"}).group("shop")
    ts = naming.ts_for(g, NOW)
    name = naming.for_group(g).physical("graph", ts)
    assert name == expect and naming.for_group(g).owns("graph", name)
    assert not naming.for_group(g).owns("graph", "customer-db")


@pytest.mark.parametrize("bad,why", [
    ({"physical_template": "{slug}"}, "must contain {ts}"),
    ({"physical_template": "{slug}-{env}-{ts}"}, "unknown field"),
    ({"physical_template": "{slug}-{ts}."}, "invalid database name"),      # trailing dot
    ({"physical_template": "Prod_{slug}-{ts}"}, "invalid database name"),  # uppercase, underscore
    ({"ts_format": "%Y-%m-%d"}, "join character"),
    ({"physical_template": "{slug}-" + "x" * 50 + "-{ts}"}, "invalid database name"),  # too long
])
def test_rejected_templates(bad, why):
    with pytest.raises(Exception, match=why):
        pol(naming=bad)


def test_group_and_profile_override_the_global_block():
    p = P.Policy.model_validate({
        "naming": {"physical_template": "prod-{slug}-{ts}"},
        "profiles": {"blue": {"tier": "gold", "naming": {"physical_template": "b-{slug}-{ts}"}}},
        "db_groups": [{"id": "a", "tier": "gold", "aliases": ["a1"]},
                      {"id": "b", "profile": "blue", "aliases": ["b1"]},
                      {"id": "c", "tier": "gold", "aliases": ["c1"],
                       "naming": {"physical_template": "c-{slug}-{ts}"}}],
        "tiers": TIERS})
    assert [p.group(x).naming.physical_template for x in "abc"] == [
        "prod-{slug}-{ts}", "b-{slug}-{ts}", "c-{slug}-{ts}"]


def test_restore_uses_the_template():
    class G:
        id, restore_mode, names = "shop", "alias-swap", ["graph"]
        naming = P.Naming(physical_template="prod-{slug}-{ts}")

        def topology_for(self, n):
            return None
    from test_choose_seed import Neo
    s = MemStore()
    s.add("shop/graph/graph-1/full.backup")
    neo = Neo()
    out = ops.restore_group(neo, s, G(), paths.DefaultPathLayout())
    assert out["members"][0]["newdb"].startswith("prod-graph-")
