"""#220: a cutover that fails part-way records exactly what moved; rollback undoes that."""

import json

import pytest

from neo4j_backup_core import adopt, ops, paths
from test_artifacts import MemStore
from test_choose_seed import Neo

L = paths.DefaultPathLayout()


class G:
    id, restore_mode, names = "g", "alias-swap", ["orders", "graph"]

    def topology_for(self, n):
        return None


class Flaky(Neo):
    def __init__(self):
        super().__init__()
        self.targets = {"orders": "orders-old", "graph": "graph-old"}

    def alias_target(self, a):
        return self.targets.get(a)

    def alter_alias(self, a, t):
        if a == "graph" and t != "graph-old":
            raise RuntimeError("leader switch")
        self.targets[a] = t

    def database_exists(self, n):
        return n.endswith("-old")


def store():
    s = MemStore()
    for n in G.names:
        s.add(f"g/{n}/{n}-old/full.backup")
    return s


def test_failure_on_the_second_alias_records_only_the_first():
    s, neo = store(), Flaky()
    with pytest.raises(ops.CutoverError) as e:
        ops.restore_group(neo, s, G(), L)
    assert e.value.moved == ["orders"] and e.value.not_moved == ["graph"]
    last = json.loads(s.get_text("_state/g.json"))["swaps"][-1]
    assert list(last["members"]) == ["orders"] and last["complete"] is False
    ops.rollback(neo, s, G())  # puts back exactly what moved
    assert neo.targets == {"orders": "orders-old", "graph": "graph-old"}


def test_a_full_cutover_is_one_complete_entry():
    s, neo = store(), Flaky()
    neo.alter_alias = lambda a, t: neo.targets.__setitem__(a, t)
    ops.restore_group(neo, s, G(), L)
    swaps = json.loads(s.get_text("_state/g.json"))["swaps"]
    assert len(swaps) == 1 and swaps[0]["complete"] and set(swaps[0]["members"]) == {"orders",
                                                                                      "graph"}


def test_adopt_records_each_member_as_it_goes():
    class A:
        id, restore_mode, names = "g", "alias-swap", ["a1", "a2"]
        overrides = {"a1": {"adopt_from": "db1"}, "a2": {"adopt_from": "db2"}}

        def topology_for(self, n):
            return None

    class N:
        def __init__(self):
            self.aliases = {}

        def alias_target(self, a):
            return self.aliases.get(a)

        def database_exists(self, n):
            return n in ("db1", "db2")

        def create_alias(self, a, t):
            if a == "a2":
                raise RuntimeError("boom")
            self.aliases[a] = t
    s = MemStore()
    with pytest.raises(RuntimeError):
        adopt.adopt(None, N(), s, None, L, A())
    last = json.loads(s.get_text("_state/g.json"))["swaps"][-1]
    assert list(last["members"]) == ["a1"] and last["complete"] is False
