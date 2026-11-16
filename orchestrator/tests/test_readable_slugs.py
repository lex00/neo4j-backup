"""#91: readable slugs (opt-in)."""

import pytest

from neo4j_backup_core import naming, paths
from neo4j_backup_core import policy as P

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


@pytest.mark.parametrize("alias,legacy,readable", [
    ("Orders", "orders-cded0933", "orders"),
    ("ACME-Graph", None, "acme-graph"),
    ("acme_orders", "acme-orders-2ac9f9a0", "acme-orders-2ac9f9a0"),  # `_` was sanitized
    ("orders.prod", "orders.prod-7c0cef44", "orders.prod-7c0cef44"),  # dot: not path-clean
    ("ab", "ab-da23614e", "ab-da23614e"),                             # too short
    ("orders", "orders", "orders"),
])
def test_readable_hashes_only_when_needed(alias, legacy, readable):
    if legacy:
        assert naming.slug(alias) == legacy  # the default is unchanged
    assert naming.slug_readable(alias) == readable


def pol(aliases, style="readable", **group):
    return P.Policy.model_validate({
        "naming": {"slug_style": style},
        "db_groups": [{"id": "shop", "tier": "gold", "aliases": aliases, **group}],
        "tiers": TIERS})


def test_layout_and_physical_names_follow_the_style():
    p = pol(["Orders"])
    layout = paths.DefaultPathLayout()
    layout.prefix_for = paths.prefix_from(p)
    assert layout.alias_prefix("shop", "Orders") == "shop/orders/"
    g = p.group("shop")
    name = naming.for_group(g).physical("Orders", "20261116t120000")
    assert name == "orders-20261116t120000" and naming.for_group(g).owns("Orders", name)
    legacy = pol(["Orders"], style="legacy")
    layout.prefix_for = paths.prefix_from(legacy)
    assert layout.alias_prefix("shop", "Orders") == "shop/orders-cded0933/"


def test_two_members_on_one_slug_fail_pointing_at_prefix():
    with pytest.raises(Exception, match=r"overrides\.<alias>\.prefix"):
        pol(["Orders", "orders"])


def test_migration_keeps_an_existing_member_on_its_folder():
    p = pol(["Orders"], overrides={"Orders": {"prefix": "shop/orders-cded0933/"}})
    layout = paths.DefaultPathLayout()
    layout.prefix_for = paths.prefix_from(p)
    assert layout.alias_prefix("shop", "Orders") == "shop/orders-cded0933/"
