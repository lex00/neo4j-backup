"""#92: adopt existing databases."""

import json

import pytest

from neo4j_backup_core import adopt, ops, paths
from test_artifacts import MemStore

L = paths.DefaultPathLayout()


class G:
    id, restore_mode = "g", "alias-swap"

    def __init__(self, names, overrides=None):
        self.names, self.overrides = names, overrides or {}

    def topology_for(self, n):
        return None


class Neo:
    def __init__(self, dbs=(), aliases=None):
        self.dbs, self.aliases = set(dbs), dict(aliases or {})
        self.log = []

    def alias_target(self, a):
        return self.aliases.get(a)

    def database_exists(self, n):
        return n in self.dbs

    def create_alias(self, a, t):
        self.log.append(("alias", a, t))
        self.aliases[a] = t

    def privilege_commands(self, d):
        return [f"GRANT ACCESS ON DATABASE `{d}` TO `app`"] if d in self.dbs else []

    def run_system(self, q, **p):
        self.log.append(("cypher", q))
        return []

    def seed_database(self, name, uri, **k):
        self.log.append(("seed", name, uri))
        self.dbs.add(name)

    def drop_database(self, n):
        self.log.append(("drop", n))
        self.dbs.discard(n)


def test_plan_create_skip_conflict():
    g = G(["orders", "graph", "audit"], {"orders": {"adopt_from": "legacy-orders"},
                                          "graph": {"adopt_from": "legacy-graph"},
                                          "audit": {"adopt_from": "legacy-audit"}})
    neo = Neo(dbs={"legacy-orders", "legacy-graph", "legacy-audit"},
              aliases={"graph": "legacy-graph", "audit": "somewhere-else"})
    plan = adopt.plan_adopt(neo, g)
    assert [m["action"] for m in plan["members"]] == ["create", "skip", "conflict"]
    with pytest.raises(ops.OpError, match="audit: conflict"):
        adopt.adopt(None, neo, MemStore(), None, L, g)
    assert not [e for e in neo.log if e[0] == "alias"]  # nothing changed


def test_alias_only_adopt_records_the_target():
    g = G(["orders"], {"orders": {"adopt_from": "legacy-orders"}})
    neo, s = Neo(dbs={"legacy-orders"}), MemStore()
    adopt.adopt(None, neo, s, None, L, g)
    assert neo.aliases == {"orders": "legacy-orders"}
    swap = json.loads(s.get_text("_state/g.json"))["swaps"][-1]
    assert swap["members"]["orders"] == {"from": None, "to": "legacy-orders"}


def test_same_name_needs_migrate_and_replace():
    neo = Neo(dbs={"orders"})
    plan = adopt.plan_adopt(neo, G(["orders"]))
    assert plan["members"][0]["action"] == "migrate" and plan["drops"] == ["orders"]
    assert "replace" in plan["needs"]
    with pytest.raises(ops.OpError, match="migrate and replace"):
        adopt.adopt(None, neo, MemStore(), None, L, G(["orders"]), migrate=True)


def test_migrate_backs_up_verifies_then_drops(monkeypatch):
    neo, s = Neo(dbs={"orders"}), MemStore()
    steps = []
    monkeypatch.setattr(ops, "backup_target", lambda *a, **k: steps.append("backup") or
                        {"artifact": "g/orders/orders/full.backup"})
    monkeypatch.setattr(ops, "verify_target", lambda *a, **k: steps.append("verify") or
                        {"consistent": True})
    monkeypatch.setattr(ops, "check_seeded", lambda *a, **k: steps.append("check"))
    out = adopt.adopt(None, neo, s, None, L, G(["orders"]), migrate=True, replace=True)
    new = out["members"][0]["newdb"]
    assert steps == ["backup", "verify", "check"]
    kinds = [e[0] for e in neo.log]
    assert kinds.index("seed") < kinds.index("drop") < kinds.index("alias")
    assert ("cypher", f"GRANT ACCESS ON DATABASE `{new}` TO `app`") in neo.log  # before drop
    assert neo.aliases == {"orders": new} and "orders" not in neo.dbs


def test_migrate_stops_if_the_backup_does_not_verify(monkeypatch):
    neo = Neo(dbs={"orders"})
    monkeypatch.setattr(ops, "backup_target", lambda *a, **k: {"artifact": "k"})
    monkeypatch.setattr(ops, "verify_target", lambda *a, **k: {"consistent": False})
    with pytest.raises(ops.OpError, match="didn't verify"):
        adopt.adopt(None, neo, MemStore(), None, L, G(["orders"]), migrate=True, replace=True)
    assert "orders" in neo.dbs and not any(e[0] == "drop" for e in neo.log)


def test_by_name_groups_are_refused():
    g = G(["orders"])
    g.restore_mode = "by-name"
    with pytest.raises(ops.OpError, match="by-name"):
        adopt.plan_adopt(Neo(), g)
