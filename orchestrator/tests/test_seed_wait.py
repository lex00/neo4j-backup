"""#221: a seed that hasn't finished never gets an alias."""

import pytest

from neo4j_backup_core import ops
from neo4j_backup_core.clients import Neo4jClient


class Rec(Neo4jClient):
    def __init__(self, rows, status=None):
        self.rows, self.q = rows, []
        self.status = status if status is not None else [{"currentStatus": "online"}]

    def run_system(self, q, **p):
        if q.startswith("SHOW DATABASE"):
            return self.status
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


def test_seed_waits_until_every_server_has_it_online(monkeypatch):
    """Seen on a 3-server cluster: Cypher 25's WAIT returns before the secondaries are online."""
    calls = iter([[{"currentStatus": "online"}, {"currentStatus": "starting"}],
                  [{"currentStatus": "online"}, {"currentStatus": "online"}]])

    class Slow(Rec):
        def run_system(self, q, **p):
            if q.startswith("SHOW DATABASE"):
                return next(calls)
            return []
    import time
    monkeypatch.setattr(time, "sleep", lambda s: None)
    Slow([]).seed_database("orders-2", "s3://b/k.backup")  # returns once both are online


def test_seed_fails_if_not_online_in_time():
    never = Rec([], status=[{"currentStatus": "starting", "statusMessage": "seeding"}])
    with pytest.raises(RuntimeError, match="isn't online on every server after 0s: starting"):
        never.wait_online("orders-2", 0, sleep=lambda s: None)
