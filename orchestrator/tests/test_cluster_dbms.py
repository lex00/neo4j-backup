"""#106 (b, c): DBMS objects and concurrency per cluster. The default cluster keeps the original
`_dbms/` keys (owner's decision); a named cluster writes under `_dbms/<cluster>/`."""

from datetime import datetime, timezone

import pytest

from neo4j_backup_core import env, ops, paths
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore

L = paths.DefaultPathLayout()
TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def pol():
    return Policy.model_validate({
        "clusters": {"eu": {"bolt_uri": "neo4j://eu:7687"}},
        "db_groups": [{"id": "shop", "aliases": ["shop"], "tier": "gold", "cluster": "eu"},
                      {"id": "local", "aliases": ["local"], "tier": "gold"}],
        "tiers": TIERS, "dbms": {"keep_metadata": 1, "keep_system": 1}})


def test_layout_keeps_default_keys_and_nests_named_clusters():
    assert L.metadata_key("T") == "_dbms/metadata-T.cypher"
    assert L.system_prefix() == "_dbms/system/"
    assert L.metadata_key("T", "eu") == "_dbms/eu/metadata-T.cypher"
    assert L.system_prefix("eu") == "_dbms/eu/system/"
    assert L.metadata_listing() == "_dbms/metadata-"


@pytest.mark.parametrize("name", ["system", "metadata", "metadata-x"])
def test_reserved_cluster_names(name):
    with pytest.raises(ValueError, match="reserved"):
        Policy.model_validate({"clusters": {name: {"bolt_uri": "neo4j://x"}}, "db_groups": [],
                               "tiers": TIERS})


def test_dbms_sweep_per_cluster_with_exact_listings():
    store = MemStore()
    for t in ("1", "2"):
        store.put_text(f"_dbms/metadata-{t}.cypher", "x")
        store.put_text(f"_dbms/eu/metadata-{t}.cypher", "x")
        store.add(f"_dbms/system/s{t}.backup")
        store.add(f"_dbms/eu/system/s{t}.backup")
    out = ops.prune(store, pol(), L, groups=[], dry_run=True,
                    now=datetime(2026, 11, 15, tzinfo=timezone.utc))
    assert sorted(out["keys"]) == ["_dbms/eu/metadata-1.cypher", "_dbms/eu/system/s1.backup",
                                   "_dbms/metadata-1.cypher", "_dbms/system/s1.backup"]
    one = ops.prune(store, pol(), L, groups=[], dry_run=True, cluster="eu",
                    now=datetime(2026, 11, 15, tzinfo=timezone.utc))
    assert all(k.startswith("_dbms/eu/") for k in one["keys"])


def test_metadata_export_and_restore_per_cluster(monkeypatch):
    monkeypatch.setattr(ops.metadata, "capture", lambda neo: {"neo": neo})
    monkeypatch.setattr(ops.metadata, "render", lambda cap, ts: f"// from {cap['neo']}")
    replayed = []
    monkeypatch.setattr(ops.metadata, "replay",
                        lambda neo, text: replayed.append((neo, text)) or {"applied": 1,
                                                                           "skipped": []})
    store = MemStore()
    neo = env.Neo4jSet("default-client", policy=pol())
    neo._built["eu"] = "eu-client"
    out = ops.export_metadata(neo, store, L, cluster="eu")
    assert out["key"].startswith("_dbms/eu/metadata-")
    assert store.objects[out["key"]] == "// from eu-client"
    assert ops.restore_metadata(neo, store, L, cluster="eu")["key"] == out["key"]
    assert replayed[0][0] == "eu-client"
    with pytest.raises(ops.OpError):  # the default cluster has no export; it never sees eu's
        ops.restore_metadata(neo, store, L)


def test_dagster_dbms_schedules_and_cluster_tags(tmp_path):
    dg = pytest.importorskip("dagster")
    import json
    import os
    import subprocess
    import sys

    import yaml
    raw = yaml.safe_load(open("policies/demo.yaml"))
    raw["clusters"] = {"eu": {"bolt_uri": "neo4j://eu:7687"}}
    raw["db_groups"].append({"id": "shop", "aliases": ["shop"], "tier": "gold", "cluster": "eu"})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    code = ("import json; from neo4j_backup_dagster import definitions as D; "
            "print(json.dumps({s.name: s.tags for s in D.defs.schedules if 'dbms' in s.name})); "
            "print(json.dumps(D._lane_request('shop/shop', 'diff').tags))")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env={**os.environ, "NEO4J_PASSWORD": "x", "NEO4J_BACKUP_POLICY": str(f)})
    assert p.returncode == 0, p.stderr
    sched, tags = (json.loads(x) for x in p.stdout.splitlines()[-2:])
    assert set(sched) == {"default_dbms_metadata", "default_dbms_system",
                          "eu_dbms_metadata", "eu_dbms_system"}
    assert sched["eu_dbms_system"] == {"neo4j_cluster": "eu"}
    assert tags == {"backup_kind": "diff", "neo4j_cluster": "eu", "neo4j_group": "shop"}
