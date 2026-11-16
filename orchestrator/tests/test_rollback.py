"""#122: rollback to the database before the last swap, from `_state/<group>.json`."""

import json

import pytest

from neo4j_backup_core import ops, paths
from test_artifacts import MemStore
from test_choose_seed import Neo

L = paths.DefaultPathLayout()


class G:
    id, restore_mode = "g", "alias-swap"
    names = ["orders"]

    def topology_for(self, n):
        return None


class N(Neo):
    def __init__(self, exists=True):
        super().__init__()
        self.target = "orders-old"
        self.exists = exists
        self.ran = []

    def alias_target(self, a):
        return self.target

    def alter_alias(self, a, t):
        super().alter_alias(a, t)
        self.target = t

    def database_exists(self, n):
        return self.exists and n == "orders-old"

    def privilege_commands(self, d):
        return [f"GRANT ACCESS ON DATABASE `{d}` TO `r`"]

    def run_system(self, q, **p):
        self.ran.append(q)
        return []


def restored():
    s = MemStore()
    s.add("g/orders/orders-1/full.backup")
    neo = N()
    out = ops.restore_group(neo, s, G(), L)
    return s, neo, out["members"][0]["newdb"]


def test_restore_records_the_swap_and_rollback_undoes_it():
    s, neo, new = restored()
    state = json.loads(s.get_text("_state/g.json"))
    assert state["swaps"][-1]["members"]["orders"] == {"from": "orders-old", "to": new}
    plan = ops.rollback(neo, s, G(), dry_run=True)
    assert plan["members"][0]["newdb"] == "orders-old" and neo.target == new
    ops.rollback(neo, s, G())
    assert neo.target == "orders-old"
    assert "GRANT ACCESS ON DATABASE `orders-old` TO `r`" in neo.ran  # privileges checked/copied
    assert json.loads(s.get_text("_state/g.json"))["swaps"][-1]["members"]["orders"]["to"] == \
        "orders-old"  # rollback is itself recorded


def test_refuses_when_the_previous_database_is_gone():
    s, neo, _new = restored()
    neo.exists = False
    with pytest.raises(ops.OpError, match="no longer exists"):
        ops.rollback(neo, s, G())


def test_refuses_without_a_recorded_swap():
    with pytest.raises(ops.OpError, match="no recorded swap"):
        ops.rollback(N(), MemStore(), G())
