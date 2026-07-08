"""seed_database statement rendering: topology, PITR, and the Cypher-version-coupled
existingData (required in Cypher 5, deprecated in Cypher 25 — Neo4j docs)."""

from neo4j_backup_core.clients import Neo4jClient
from neo4j_backup_core.policy import Topology


class _CaptureClient(Neo4jClient):
    def run_system(self, cypher, **params):  # don't hit a real server
        self.last = cypher
        return []


def _seed(**kw) -> str:
    c = _CaptureClient("neo4j://x", "neo4j", "pw")
    c.seed_database("orders-t1", "s3://b/k.backup", **kw)
    return c.last


def test_default_no_prefix_no_existing_data():
    s = _seed()
    assert s == "CREATE DATABASE `orders-t1` OPTIONS { seedURI: 's3://b/k.backup' } WAIT"
    assert "existingData" not in s and "CYPHER" not in s


def test_cypher5_pins_language_and_requires_existing_data():
    s = _seed(cypher_version="5")
    assert s.startswith("CYPHER 5 CREATE DATABASE `orders-t1`")
    assert "existingData: 'use'" in s


def test_cypher25_pins_language_and_omits_existing_data():
    s = _seed(cypher_version="25")
    assert s.startswith("CYPHER 25 CREATE DATABASE `orders-t1`")
    assert "existingData" not in s


def test_topology_clause_before_options():
    s = _seed(topology=Topology(primaries=3, secondaries=0))
    assert "TOPOLOGY 3 PRIMARIES 0 SECONDARIES OPTIONS" in s


def test_cypher5_with_topology_and_pitr():
    s = _seed(cypher_version="5", topology=Topology(primaries=3, secondaries=1),
              restore_until="2026-06-29T11:00:00Z")
    assert s.startswith("CYPHER 5 CREATE DATABASE `orders-t1` TOPOLOGY 3 PRIMARIES 1 SECONDARIES OPTIONS")
    assert "existingData: 'use'" in s
    assert "seedRestoreUntil: datetime('2026-06-29T11:00:00Z')" in s
