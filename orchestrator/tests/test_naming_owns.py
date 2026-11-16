"""#87: physical() keeps the alias hash; owns() recognises the tool's own physicals."""

import re

import pytest

from neo4j_backup_core import naming

TS = "20261115t120000"


@pytest.mark.parametrize("n", range(1, 201, 7))
def test_every_length_gives_a_legal_name_with_the_hash(n):
    a = "Customer Graph " + "x" * n
    p = naming.physical(a, TS)
    naming.validate_db(p)
    assert len(p) <= 63 and p.endswith(f"-{naming._hash8(a)}-{TS}")
    assert naming.owns(a, p)


def test_the_issue_example():
    a = "Customer Graph (EU) – production instance number 42"
    p = naming.physical(a, TS)
    assert re.fullmatch(r"customer-graph-eu-production-insta[a-z-]*-9743a837-20261115t120000", p)


def test_owns():
    assert naming.owns("acme-orders", f"acme-orders-{TS}")
    assert not naming.owns("acme-orders", f"acme-orders-x-{TS}")
    assert not naming.owns("acme-orders", "acme-orders")
    assert not naming.owns("acme-orders", f"acme-ordersx-{TS}")
    assert not naming.owns("Acme Orders", f"acme-orders-deadbeef-{TS}")  # wrong hash
    assert naming.owns("Acme Orders", naming.physical("Acme Orders", TS))
    assert not naming.owns("acme-orders", "customer-made-db")


# --- #85: Neo4j's database-name rules ----------------------------------------------------------

@pytest.mark.parametrize("ok", ["abc", "orders.eu", "ab-", "orders-", "a" * 63])
def test_legal_names(ok):
    naming.validate_db(ok)


@pytest.mark.parametrize("bad", ["2024-orders", "ab", "a" * 64, "_x1", "system-x", "Orders",
                                 "a_b"])
def test_illegal_names(bad):
    with pytest.raises(naming.NamingError):
        naming.validate_db(bad)


def test_digit_led_slug_keeps_its_prefix_and_gets_db_in_the_physical():
    from neo4j_backup_core import paths
    assert paths.alias_prefix("g", "2024-orders") == "g/2024-orders/"   # storage unchanged
    p = naming.physical("2024-orders", TS)
    assert p == f"db-2024-orders-{TS}" and naming.owns("2024-orders", p)


def test_by_name_databases_are_lowercased():
    from neo4j_backup_core.policy import DbGroup
    g = DbGroup(id="g", restore_mode="by-name", databases=["Orders"], tier="gold")
    assert g.names == ["orders"]
