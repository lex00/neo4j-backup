"""#111: checks on a new database before its swap."""

import pytest

from neo4j_backup_core import checks, ops, paths
from neo4j_backup_core.policy import Check
from test_artifacts import MemStore


@pytest.mark.parametrize("expect,yes,no", [("== 3", 3, 4), (">= 100", 100, 99), ("<= 5", 5, 6),
                                           ("between 10 20", 15, 21)])
def test_expectations(expect, yes, no):
    p = checks.parse_expect(expect)
    assert p(yes) and not p(no)


@pytest.mark.parametrize("bad", ["> 3", "== x", "between 1", "== 1 2", ""])
def test_bad_expectations(bad):
    with pytest.raises(ValueError):
        checks.parse_expect(bad)
    with pytest.raises(ValueError):
        Check(query="RETURN 1", expect=bad)


def test_import_counts():
    out = "IMPORT DONE in 746ms.\nImported:\n  3 nodes\n  2 relationships\n  6 properties\n"
    assert checks.import_counts(out) == {"nodes": 3, "relationships": 2}
    assert checks.import_counts("nothing") is None
    new = ("Importing nodes\nImported Stats[processed=3, created=3, updated=0, deleted=0] nodes in "
           "160ms\nImporting relationships\nImported Stats[processed=1, created=1, updated=0, "
           "deleted=0] relationships in 104ms\n")  # 2026.05, seen on the stack
    assert checks.import_counts(new) == {"nodes": 3, "relationships": 1}


class Neo:
    def __init__(self, nodes=3, rels=2, status="online", extra=5):
        self.nodes, self.rels, self.status, self.extra = nodes, rels, status, extra
        self.dropped = []

    def run_system(self, q, **p):
        return [{"currentStatus": self.status}]

    def run_on(self, db, q):
        if "()-[r]->()" in q:
            return [{"n": self.rels}]
        if q == "MATCH (n) RETURN count(n) AS n":
            return [{"n": self.nodes}]
        return [{"v": self.extra}]

    def drop_database(self, n):
        self.dropped.append(n)


def test_default_and_policy_checks_pass():
    res = checks.run(Neo(), "db", [Check(query="RETURN 5", expect=">= 5", name="products")],
                     expect_counts={"nodes": 3, "relationships": 2})
    assert [r["name"] for r in res] == ["online", "nodes", "nodes match import",
                                        "relationships match import", "products"]


@pytest.mark.parametrize("neo,counts,failing", [
    (Neo(status="offline"), None, "online"),
    (Neo(nodes=0), None, "nodes"),
    (Neo(), {"nodes": 4, "relationships": 2}, "nodes match import"),
    (Neo(extra=1), None, "check 1"),
])
def test_failures_name_the_check_and_value(neo, counts, failing):
    with pytest.raises(checks.CheckFailed) as e:
        checks.run(neo, "db", [{"query": "RETURN 1", "expect": ">= 5"}], expect_counts=counts)
    assert e.value.results[-1]["name"] == failing and not e.value.results[-1]["ok"]


def test_failed_restore_check_drops_the_new_databases_and_moves_no_alias():
    class G:
        id = "g"
        names = ["orders"]
        restore_mode = "alias-swap"

        class load:
            checks = [Check(query="RETURN 1", expect=">= 5")]

        def topology_for(self, n):
            return None

    class N(Neo):
        def __init__(self):
            super().__init__(extra=1)
            self.altered = []

        def alias_target(self, a):
            return "orders-old"

        def seed_database(self, *a, **k):
            pass

        def alter_alias(self, a, t):
            self.altered.append(a)

        def privilege_commands(self, d):
            return []
    s = MemStore()
    s.add("g/orders/orders-1/full.backup")
    neo = N()
    with pytest.raises(ops.OpError, match="no alias was moved"):
        ops.restore_group(neo, s, G(), paths.DefaultPathLayout(), check=True)
    assert neo.altered == [] and len(neo.dropped) == 1
