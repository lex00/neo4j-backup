"""#206: Neo4j stores alias names lowercase; lookups must not depend on the policy's spelling."""

from neo4j_backup_core import metadata, move
from neo4j_backup_core.clients import Neo4jClient


class Recording(Neo4jClient):
    def __init__(self, rows):
        self.queries, self.rows = [], rows

    def run_system(self, q, **p):
        self.queries.append((q, p))
        return self.rows


def test_alias_and_database_lookups_ignore_case():
    c = Recording([{"database": "orders-1", "name": "orders-1"}])
    assert c.alias_target("MixCase") == "orders-1"
    assert "toLower(name) = toLower($a)" in c.queries[-1][0]
    assert c.database_exists("Orders-1")
    assert "toLower(name) = toLower($n)" in c.queries[-1][0]


def test_move_finds_a_mixed_case_alias_in_the_export():
    snap = {"roles": [], "users": [], "memberships": [], "privileges": [],
            "aliases": [{"name": "mixcase", "database": "mixcase-1"}]}  # as SHOW ALIASES lists it
    meta = move.split_metadata(metadata.render(snap))
    assert meta["aliases"].get("MixCase".lower()) == "mixcase-1"
