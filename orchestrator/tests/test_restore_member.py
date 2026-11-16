"""#121: restore one member of a group."""

import pytest

from neo4j_backup_core import ops, paths
from test_artifacts import MemStore
from test_choose_seed import Neo

L = paths.DefaultPathLayout()


class G:
    id, restore_mode = "g", "alias-swap"
    names = ["orders", "graph", "audit"]

    def topology_for(self, n):
        return None


def store():
    s = MemStore()
    for n in G.names:
        s.add(f"g/{n}/{n}-1/full.backup")
    return s


def test_one_member_of_three():
    neo = Neo()
    out = ops.restore_group(neo, store(), ops.only_members(G(), ["graph"]), L)
    assert [m["alias"] for m in out["members"]] == ["graph"]
    assert len(neo.seeded) == 1 and neo.altered == [("graph", out["members"][0]["newdb"])]


def test_plan_warns_about_alignment():
    plan = ops.plan_restore(Neo(), store(), ops.only_members(G(), ["graph"]), L)
    assert "no longer be aligned" in plan["warning"] and "'orders'" in plan["warning"]
    assert "warning" not in ops.plan_restore(Neo(), store(), G(), L)


def test_unknown_member_is_refused():
    with pytest.raises(ops.OpError, match="not members"):
        ops.only_members(G(), ["nope"])


def test_baseline_only_for_the_restored_member():
    seen = []

    def run(cmd, env=None):
        seen.append(cmd)
        return "neo4j 2026.05.0\n[]"
    sub = ops.only_members(G(), ["audit"])
    from neo4j_backup_core.clients import BackupRunner
    done = ops.baseline_after_restore(run, Neo(), store(), BackupRunner(), L, sub,
                                      [{"alias": "audit", "newdb": "audit-2"}])
    assert [d["member"] for d in done] == ["audit"]
