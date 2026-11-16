"""#115: restore a group into another cluster."""

import json

import pytest

from neo4j_backup_core import metadata, move, ops, paths
from test_artifacts import MemStore

L = paths.DefaultPathLayout()
SNAP = {"roles": ["app"], "users": [{"name": "alice"}], "memberships": [("app", "alice")],
        "privileges": ["GRANT ACCESS ON DATABASE * TO `app`",
                       "GRANT MATCH {*} ON GRAPH `orders-old` NODE * TO `app`",
                       "GRANT MATCH {*} ON GRAPH `other-db` NODE * TO `app`"],
        "aliases": [{"name": "orders", "database": "orders-old"},
                    {"name": "other", "database": "other-db"}]}


class G:
    id, restore_mode, names = "g", "alias-swap", ["orders"]

    def topology_for(self, n):
        return None


class CNeo:
    def __init__(self, up=True, aliases=None, privs=None):
        self.up, self.aliases, self.privs = up, aliases or {}, privs or {}
        self.ran, self.seeded, self.created, self.altered = [], [], [], []

    def list_databases(self):
        if not self.up:
            raise ConnectionError("down")
        return ["neo4j", *self.aliases.values()]

    def alias_target(self, a):
        return self.aliases.get(a)

    def privilege_commands(self, d):
        return self.privs.get(d, [])

    def run_system(self, q, **p):
        self.ran.append(q)
        return []

    def seed_database(self, name, uri, **k):
        self.seeded.append((name, uri))

    def database_exists(self, n):
        return False

    def create_alias(self, a, t):
        self.created.append((a, t))
        self.aliases[a] = t

    def alter_alias(self, a, t):
        self.altered.append((a, t))
        self.aliases[a] = t


class Clusters:
    def __init__(self, **c):
        self.c = c

    def named(self, n):
        return self.c[n]


def store(with_export=True):
    s = MemStore()
    s.add("g/orders/orders-old/full.backup")
    if with_export:
        s.put_text(L.metadata_key("20261115t000000", "east"), metadata.render(SNAP))
    return s


def test_lost_cluster_moves_from_the_metadata_export():
    east, west = CNeo(up=False), CNeo()
    s = store()
    out = move.move(Clusters(east=east, west=west), s, L, [G()], "east", "west",
                    clusters=["east", "west"])
    new = west.seeded[0][0]
    assert west.created == [("orders", new)]  # the alias is created on the target
    assert "CREATE ROLE `app` IF NOT EXISTS" in west.ran
    assert "GRANT ACCESS ON DATABASE * TO `app`" in west.ran
    assert f"GRANT MATCH {{*}} ON GRAPH `{new}` NODE * TO `app`" in west.ran  # rewritten
    assert not any("other-db" in q for q in west.ran)  # other groups' grants stay behind
    assert not any(q.startswith("CREATE ALIAS") for q in west.ran)
    assert "set `cluster: west`" in out["policy"]
    assert json.loads(s.get_text("_state/g.json"))["fenced"]["cluster"] == "east"


def test_refuses_while_the_source_answers_unless_fenced(monkeypatch):
    east = CNeo(aliases={"orders": "orders-old"},
                privs={"orders-old": ["GRANT MATCH {*} ON GRAPH `orders-old` NODE * TO `app`"]})
    west = CNeo()
    with pytest.raises(ops.OpError, match="fence-source"):
        move.plan_move(Clusters(east=east, west=west), store(), L, [G()], "east", "west")
    monkeypatch.setattr(metadata, "capture", lambda neo: SNAP)
    plan = move.plan_move(Clusters(east=east, west=west), store(False), L, [G()], "east", "west",
                          fence_source=True)
    assert plan["metadata"]["from"] == "live" and plan["source_reachable"]
    m = plan["groups"][0]["members"][0]
    assert m["source_physical"] == "orders-old" and len(m["privileges"]) == 1
    assert west.seeded == [] and west.ran == []  # the plan changes nothing


def test_preflight_failures():
    c = Clusters(east=CNeo(up=False), west=CNeo(up=False))
    with pytest.raises(ops.OpError, match="not in the policy"):
        move.plan_move(c, store(), L, [G()], "east", "south", clusters=["east", "west"])
    with pytest.raises(ops.OpError, match="doesn't answer"):
        move.plan_move(c, store(), L, [G()], "east", "west")
    c = Clusters(east=CNeo(up=False), west=CNeo())
    with pytest.raises(ops.OpError, match="no metadata export"):
        move.plan_move(c, store(False), L, [G()], "east", "west")
    assert move.plan_move(c, store(False), L, [G()], "east", "west",
                          skip_metadata=True)["metadata"] is None


def test_fence_stops_lanes_on_the_source_until_the_policy_moves():
    class Set:
        def __init__(self, cluster):
            self.cluster = cluster

        def _pol(self):
            c = self.cluster

            class P:
                def cluster_name(self, gid):
                    return c
            return P()
    s = MemStore()
    ops.fence(s, "g", "east", "west")
    with pytest.raises(ops.OpError, match="cluster: west"):
        ops.check_fence(Set("east"), s, "g")
    ops.check_fence(Set("west"), s, "g")  # the policy now says west


def test_groups_for_a_whole_cluster():
    class P:
        db_groups = [type("X", (), {"id": i})() for i in ("a", "b", "c")]

        def cluster_name(self, gid):
            return {"a": "east", "b": "west", "c": "east"}[gid]

        def group(self, gid):
            return next(g for g in self.db_groups if g.id == gid)
    gs, src = move.groups_for(P(), None, "east")
    assert [g.id for g in gs] == ["a", "c"] and src == "east"
    gs, src = move.groups_for(P(), "b", None)
    assert [g.id for g in gs] == ["b"] and src == "west"
