"""#217: the Dagster resources are policy-aware, so the fence stops their backups."""

import pytest

pytest.importorskip("dagster")

from neo4j_backup_core import ops  # noqa: E402
from neo4j_backup_dagster.resources import Neo4jResource, ObjectStoreResource  # noqa: E402
from test_artifacts import MemStore  # noqa: E402

POLICY = """db_groups:
- {id: shop, tier: gold, aliases: [shop]}
tiers: {gold: {full_cron: '0 2 * * *', diff_cron: '0 * * * *'}}
"""


def test_dagster_resources_hit_the_fence(tmp_path, monkeypatch):
    f = tmp_path / "p.yaml"
    f.write_text(POLICY)
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    s = MemStore()
    ops.fence(s, "shop", "default", "west")  # moved off `default`; the policy still says default
    neo = Neo4jResource(uri="neo4j://x:7687", user="neo4j", password="p")
    with pytest.raises(ops.OpError, match="cluster: west"):
        ops.check_fence(neo, s, "shop")
    store = ObjectStoreResource(bucket="b")
    with pytest.raises(ops.OpError, match="cluster: west"):
        ops.check_fence(store, s, "shop")


def test_backup_target_with_the_dagster_resource_refuses(tmp_path, monkeypatch):
    f = tmp_path / "p.yaml"
    f.write_text(POLICY)
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    s = MemStore()
    ops.fence(s, "shop", "default", "west")
    neo = Neo4jResource(uri="neo4j://x:7687", user="neo4j", password="p")
    with pytest.raises(ops.OpError, match="moved"):
        ops.backup_target(lambda *a, **k: "", neo, s, None, None, "shop", "shop", "FULL")
