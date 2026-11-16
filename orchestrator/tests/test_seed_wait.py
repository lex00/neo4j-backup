"""#221: a seed that hasn't finished never gets an alias."""

import pytest

from neo4j_backup_core import ops
from neo4j_backup_core.clients import Neo4jClient


class Rec(Neo4jClient):
    def __init__(self, rows):
        self.rows, self.q = rows, []

    def run_system(self, q, **p):
        self.q.append(q)
        return self.rows


def test_seed_waits_and_checks_every_row():
    ok = Rec([{"address": "a:7687", "state": "CaughtUp", "message": "", "success": True}])
    ok.seed_database("orders-2", "s3://b/k.backup", wait_seconds=7200)
    assert ok.q[-1].endswith("WAIT 7200 SECONDS")
    slow = Rec([{"address": "a:7687", "state": "CaughtUp", "success": True},
                {"address": "b:7687", "state": "InProgress", "message": "seeding", "success": False}])
    with pytest.raises(RuntimeError, match="didn't finish within 3600s: b:7687: InProgress"):
        slow.seed_database("orders-2", "s3://b/k.backup")


def test_seed_wait_comes_from_rto():
    class G:
        rto_minutes = 120
    assert ops._seed_wait(G()) == 7200


def test_no_alias_moves_until_every_new_database_is_online():
    class N:
        def __init__(self):
            self.moved = []

        def database_online(self, n):
            return n != "graph-2"

        def privilege_commands(self, d):
            return []

        def alter_alias(self, a, t):
            self.moved.append(a)
    neo = N()
    seeded = [{"alias": "orders", "newdb": "orders-2", "old": "orders-1"},
              {"alias": "graph", "newdb": "graph-2", "old": "graph-1"}]
    with pytest.raises(ops.OpError, match="graph-2 isn't online"):
        ops.cutover_seeded(neo, seeded)
    assert neo.moved == []
