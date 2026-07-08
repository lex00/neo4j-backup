"""resolve_physical / database_exists — no live cluster."""

from neo4j_backup_core.clients import Neo4jClient


class _StubResolve(Neo4jClient):
    """Stubs the two lookups resolve_physical composes."""

    def __init__(self, alias_map, databases):
        self._alias_map = alias_map
        self._databases = set(databases)

    def alias_target(self, alias):
        return self._alias_map.get(alias)

    def database_exists(self, name):
        return name in self._databases


def test_resolve_physical_from_alias():
    c = _StubResolve({"orders": "orders-t1"}, {"orders-t1"})
    assert c.resolve_physical("orders") == "orders-t1"


def test_resolve_physical_from_physical_name():
    # not an alias, but a real database -> returns itself
    c = _StubResolve({}, {"orders-t1"})
    assert c.resolve_physical("orders-t1") == "orders-t1"


def test_resolve_physical_unknown_is_none():
    c = _StubResolve({}, set())
    assert c.resolve_physical("nope") is None


class _StubExists(Neo4jClient):
    def __init__(self, existing):
        self._existing = set(existing)

    def run_system(self, cypher, **params):
        assert "SHOW DATABASES" in cypher
        return [{"name": params["n"]}] if params.get("n") in self._existing else []


def test_database_exists():
    c = _StubExists({"foo"})
    assert c.database_exists("foo") is True
    assert c.database_exists("bar") is False
