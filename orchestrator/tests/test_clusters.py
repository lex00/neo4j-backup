"""#106: one policy, several clusters; each group's ops hit its own cluster."""

import pytest

from neo4j_backup_core import env, ops, paths
from neo4j_backup_core.clients import BackupRunner
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}
LAYOUT = paths.DefaultPathLayout()


def pol(**extra):
    return Policy.model_validate({
        "storage": {"eu": {"url": "s3://backups-eu"}},
        "clusters": {"us-east": {"bolt_uri": "neo4j://use1:7687",
                                 "backup_source": ["use1-a:6362", "use1-b:6362"],
                                 "topology": {"primaries": 3}, "cypher_version": "5"},
                     "eu": {"bolt_uri": "neo4j://eu:7687", "storage": "eu",
                            "cutover": {"strategy": "external", "hook": "true"}}},
        "db_groups": [{"id": "orders", "aliases": ["orders"], "tier": "gold", "cluster": "us-east"},
                      {"id": "shop", "aliases": ["orders"], "tier": "gold", "cluster": "eu"},
                      {"id": "local", "aliases": ["local"], "tier": "gold"}],
        "tiers": TIERS, **extra})


def test_cluster_defaults_and_store_precedence():
    p = pol()
    assert p.cluster_name("local") == "default" and p.cluster_name("orders") == "us-east"
    assert p.store_name("shop") == "eu"            # the cluster's store
    assert p.store_name("orders") == "default"
    assert p.clusters["us-east"].backup_sources == "use1-a:6362,use1-b:6362"


def test_group_storage_beats_cluster_storage():
    raw = pol().model_dump()
    raw["db_groups"][1]["storage"] = "default"
    assert Policy.model_validate(raw).store_name("shop") == "default"


@pytest.mark.parametrize("change,msg", [
    (lambda r: r["db_groups"].append(dict(r["db_groups"][0])), "used twice"),
    (lambda r: r["db_groups"].append({"id": "x", "aliases": ["orders"], "tier": "gold",
                                      "cluster": "us-east"}), "belongs to one group"),
    (lambda r: r["db_groups"][0].update(cluster="nope"), "not defined"),
    (lambda r: r["clusters"].update(system={"bolt_uri": "neo4j://x"}), "reserved"),
])
def test_validation(change, msg):
    raw = pol().model_dump()
    change(raw)
    with pytest.raises(ValueError, match=msg):
        Policy.model_validate(raw)


def test_same_alias_on_two_clusters_is_fine():
    pol()  # 'orders' is a member on us-east and on eu


def test_each_group_gets_its_clusters_client_and_runner():
    p = pol()
    neo = env.Neo4jSet(env.Neo4jClient("neo4j://default:7687", "neo4j", "x"), policy=p)
    run = env.RunnerSet(BackupRunner(backup_source="default:6362"), policy=p)
    assert neo.for_group("orders").uri == "neo4j://use1:7687"
    assert neo.for_group("shop").uri == "neo4j://eu:7687"
    assert neo.for_group("local").uri == "neo4j://default:7687"
    assert run.for_group("orders").backup_source == "use1-a:6362,use1-b:6362"
    assert run.for_group("local").backup_source == "default:6362"
    assert neo.for_group("orders").cypher_version == "5"


class FakeNeo:
    def __init__(self, name):
        self.name, self.seeded, self.altered = name, [], []
        self.cypher_version = None

    def alias_target(self, a):
        return f"{a}-old"

    def seed_database(self, n, uri, restore_until=None, topology=None, cypher_version=None,
                      store_base=None):
        self.seeded.append((n, topology, cypher_version))

    def privilege_commands(self, db):
        return []

    def alter_alias(self, a, t):
        self.altered.append((a, t))

    def resolve_physical(self, a):
        return f"{a}-1"


def test_restore_runs_on_the_groups_cluster(monkeypatch):
    p = pol()
    use1, default = FakeNeo("use1"), FakeNeo("default")
    use1.cypher_version = "5"
    use1.cluster_topology = p.clusters["us-east"].topology
    neo = env.Neo4jSet(default, policy=p)
    neo._built["us-east"] = use1
    store = MemStore()
    store.add("orders/orders/orders-1/full.backup")
    ops.restore_group(neo, store, p.group("orders"), LAYOUT)
    assert use1.seeded and not default.seeded
    name, topology, cv = use1.seeded[0]
    assert topology.primaries == 3 and cv == "5"
    assert use1.altered and not default.altered


def test_targets_and_status_show_the_cluster(monkeypatch):
    from neo4j_backup_mcp import tools
    p = pol()
    monkeypatch.setattr(tools, "load_policy", lambda _p: p)
    assert tools.list_targets()["clusters"] == {"orders": "us-east", "shop": "eu",
                                                "local": "default"}
