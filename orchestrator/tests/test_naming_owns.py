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
