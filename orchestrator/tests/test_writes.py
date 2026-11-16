"""#112: enforce `writes` and `protect_writes`."""

import pytest

from neo4j_backup_core import ops, paths
from neo4j_backup_core.clients import BackupRunner, Neo4jClient
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore

L = paths.DefaultPathLayout()
TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"},
         "nightly": {"rebuild_cron": "0 1 * * *"}}
BUILD = {"kind": "import", "input_type": "csv"}


def group(load, tier="gold"):
    return Policy.model_validate({"db_groups": [{"id": "g", "aliases": ["a"], "tier": tier,
                                                 "load": load}], "tiers": TIERS}).group("g")


class Neo:
    def __init__(self, access="read-write"):
        self.access, self.ro = access, []

    def database_access(self, n):
        return self.access

    def set_read_only(self, n, ro=True):
        self.ro.append(n)

    def resolve_physical(self, a):
        return "a-1"


@pytest.mark.parametrize("load,tier,access,expect", [
    ({}, "gold", "read-write", True),                                        # live
    ({"mode": "once"}, "gold", "read-write", True),                           # then live
    ({"mode": "once", "writes": "read-only"}, "gold", "read-only", False),    # static, applied
    ({"mode": "once", "writes": "read-only"}, "gold", "read-write", True),    # not yet applied
    ({"mode": "rebuild", "writes": "read-write", "protect_writes": False, "build": BUILD},
     "nightly", "read-write", False),                                       # disposable writes
    ({"mode": "rebuild", "writes": "read-only", "build": BUILD}, "nightly", "read-only", False),
])
def test_lane_decisions(load, tier, access, expect):
    assert ops.backs_up(Neo(access), group(load, tier), "a-1")["backs_up"] is expect


def test_backup_target_skips_by_design_without_running_neo4j_admin():
    calls = []
    out = ops.backup_target(calls.append, Neo("read-only"), MemStore(), BackupRunner(), L, "g", "a",
                            "DIFF", group=group({"mode": "once", "writes": "read-only"}))
    assert out["skipped"] and out["by_design"] and calls == []


def test_read_only_groups_are_set_read_only_before_the_swap():
    neo = Neo()
    assert ops.apply_access(neo, group({"mode": "once", "writes": "read-only"}), "a-2") == \
        "read-only"
    assert neo.ro == ["a-2"]
    assert ops.apply_access(neo, group({}), "a-3") is None and neo.ro == ["a-2"]


def test_alter_statement():
    class C(Neo4jClient):
        def __init__(self):
            super().__init__()
            self.q = []

        def run_system(self, q, **p):
            self.q.append(q)
            return [{"access": "read-only"}]
    c = C()
    c.set_read_only("we`ird")
    c.set_read_only("x", False)
    assert c.q == ["ALTER DATABASE `we``ird` SET ACCESS READ ONLY",
                   "ALTER DATABASE `x` SET ACCESS READ WRITE"]
    assert c.database_access("x") == "read-only"


def test_turning_protect_writes_on_starts_with_a_full():
    # no backups in the folder (writes were not protected), so the diff lane asks for a full (#74)
    class N(Neo):
        def database_created(self, n):
            return None
    assert ops.needs_full(N(), MemStore(), L, "g", "a") is True
