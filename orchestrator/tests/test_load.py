"""#107: load / rebuild a group (core op, with fakes)."""

import pytest

from neo4j_backup_core import load as L
from neo4j_backup_core import ops, paths
from neo4j_backup_core.clients import BackupRunner
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore

LAYOUT = paths.DefaultPathLayout()
TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"},
         "nightly": {"rebuild_cron": "0 1 * * *"}}
IMPORT_OUT = ("Imported Stats[processed=3, created=3, updated=0, deleted=0] nodes in 1ms\n"
              "Imported Stats[processed=1, created=1, updated=0, deleted=0] relationships in 1ms\n")


def group(writes="read-only", checks=(), names=("catalog",), mode="rebuild", restore="alias-swap"):
    key = "databases" if restore == "by-name" else "aliases"
    load = {"mode": mode, "writes": writes,
            "build": {"kind": "import", "input_type": "csv",
                      "args": ["--nodes=Product=products.csv", "--relationships=IN=in.csv"]},
            "checks": list(checks)}
    if writes == "read-write":
        load["protect_writes"] = False
    return Policy.model_validate({"db_groups": [{"id": "cat", key: list(names), "tier": "nightly",
                                                 "restore_mode": restore, "load": load}],
                                  "tiers": TIERS}).group("cat")


class Neo:
    def __init__(self, nodes=3, rels=1):
        self.nodes, self.rels = nodes, rels
        self.seeded, self.dropped, self.altered, self.ro, self.ran = [], [], [], [], []

    def alias_target(self, a):
        return f"{a}-old"

    def database_exists(self, n):
        return False

    def seed_database(self, n, uri, **k):
        self.seeded.append((n, uri))

    def set_read_only(self, n, ro=True):
        self.ro.append(n)

    def database_access(self, n):
        return "read-only" if n in self.ro else "read-write"

    def run_system(self, q, **p):
        self.ran.append(q)
        return [{"currentStatus": "online"}]

    def run_on(self, db, q):
        return [{"n": self.rels if "()-[r]->()" in q else self.nodes}]

    def drop_database(self, n):
        self.dropped.append(n)

    def privilege_commands(self, d):
        return []

    def alter_alias(self, a, t):
        self.altered.append((a, t))


def run_admin():
    calls = []
    return calls, lambda cmd, env=None: calls.append(cmd) or IMPORT_OUT


def test_resolve_args():
    assert L.resolve_args(["--nodes=P=a.csv,/abs.csv", "--relationships=s3://x/r.csv", "--x=y"],
                          lambda p: "s3://b/src/" + p) == \
        ["--nodes=P=s3://b/src/a.csv,/abs.csv", "--relationships=s3://x/r.csv", "--x=y"]


def test_source_prefix_per_member():
    assert L.source_prefix(group(), "catalog") == "cat/_source/"
    from neo4j_backup_core import naming  # the member's storage slug, as in its backup folder
    assert L.source_prefix(group(names=("a", "b")), "a") == f"cat/_source/{naming.slug('a')}/"


def test_plan_shows_source_build_target_and_loss():
    p = L.plan_load(Neo(), MemStore(), LAYOUT, group(writes="read-write"))
    m = p["members"][0]
    assert m["source"] == "s3://bucket/cat/_source/" and m["old"] == "catalog-old"
    assert "--nodes=Product=s3://bucket/cat/_source/products.csv" in m["build"]
    assert m["loses"] == "writes since the previous load"


def test_live_groups_have_nothing_to_load():
    g = Policy.model_validate({"db_groups": [{"id": "x", "aliases": ["a"], "tier": "gold"}],
                               "tiers": TIERS}).group("x")
    with pytest.raises(ops.OpError, match="live"):
        L.plan_load(Neo(), MemStore(), LAYOUT, g)


def test_load_builds_seeds_checks_sets_read_only_and_swaps():
    neo, store = Neo(), MemStore()
    calls, run = run_admin()
    out = L.load_group(run, BackupRunner(), neo, store, LAYOUT, group())
    m = out["members"][0]
    script = calls[0][2]
    assert "database import full load-catalog-" in script and "database dump" in script
    assert "--nodes=Product=s3://bucket/cat/_source/products.csv" in script
    assert m["dump"].startswith("cat/catalog/_seed/") and m["import_counts"] == \
        {"nodes": 3, "relationships": 1}
    assert neo.seeded[0][1] == "s3://bucket/" + m["dump"]
    assert neo.ro == [m["newdb"]]                       # read-only before the swap (#112)
    assert [c["name"] for c in m["checks"]][:3] == ["online", "nodes", "nodes match import"]
    assert neo.altered == [("catalog", m["newdb"])]
    assert out["baseline"][0]["taken"] is None           # read-only: not backed up by design


def test_failed_check_drops_and_moves_no_alias():
    neo = Neo(nodes=2)  # the import said 3
    _calls, run = run_admin()
    with pytest.raises(L.LoadError) as e:
        L.load_group(run, BackupRunner(), neo, MemStore(), LAYOUT, group())
    assert e.value.step == "checks" and neo.altered == [] and len(neo.dropped) == 1


def test_failed_build_names_the_step():
    neo = Neo()

    def boom(cmd, env=None):
        raise RuntimeError("import failed")
    with pytest.raises(L.LoadError) as e:
        L.load_group(boom, BackupRunner(), neo, MemStore(), LAYOUT, group())
    assert e.value.step == "build catalog" and neo.seeded == []


def test_reapply_access():
    class N(Neo):
        def resolve_physical(self, a):
            return f"{a}-1"
    neo = N()
    out = L.reapply_access(neo, group(mode="once"))
    assert out == [{"member": "catalog", "physical": "catalog-1", "access": "read-only"}]
