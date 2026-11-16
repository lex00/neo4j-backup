"""#89: NAMING=module.Class, a pluggable naming scheme."""

import pytest

from neo4j_backup_core import naming, ops, paths
from test_artifacts import MemStore


class BlueGreen:
    """Example scheme: flip between <alias>-blue and <alias>-green."""

    def slug(self, alias):
        return alias

    def physical(self, alias, ts, current=None):
        return f"{alias}-green" if current == f"{alias}-blue" else f"{alias}-blue"

    def owns(self, alias, name):
        return name in (f"{alias}-blue", f"{alias}-green")


class Bad(BlueGreen):
    def physical(self, alias, ts, current=None):
        return "Not_Legal"


@pytest.fixture
def use(monkeypatch):
    def _use(cls):
        monkeypatch.setenv("NAMING", f"{__name__}.{cls}")
        monkeypatch.syspath_prepend(__file__.rsplit("/", 1)[0])
        naming.reset_scheme()
    yield _use
    naming.reset_scheme()


def test_default_scheme_matches_the_module_functions():
    naming.reset_scheme()
    s = naming.scheme()
    assert s.slug("Acme Orders") == naming.slug("Acme Orders")
    assert s.physical("orders", "20261115t120000") == "orders-20261115t120000"
    assert s.owns("orders", "orders-20261115t120000")


def test_custom_scheme_is_used_by_restore(use):
    use("BlueGreen")

    class G:
        id, restore_mode, names = "g", "alias-swap", ["orders"]

        def topology_for(self, n):
            return None

    class N:
        def __init__(self, current):
            self.current, self.seeded, self.alt = current, [], []

        def alias_target(self, a):
            return self.current

        def database_exists(self, n):
            return n == self.current

        def seed_database(self, n, *a, **k):
            self.seeded.append(n)

        def privilege_commands(self, d):
            return []

        def alter_alias(self, a, t):
            self.alt.append(t)
    s = MemStore()
    s.add("g/orders/orders-blue/full.backup")
    neo = N("orders-blue")
    ops.restore_group(neo, s, G(), paths.DefaultPathLayout())
    assert neo.seeded == ["orders-green"] and neo.alt == ["orders-green"]
    assert paths.DefaultPathLayout().alias_prefix("g", "orders") == "g/orders/"


def test_a_name_in_use_is_refused(use):
    use("BlueGreen")

    class N:
        def alias_target(self, a):
            return "orders-blue"

        def database_exists(self, n):
            return True  # orders-green still exists (not cleaned up)

    class G:
        id, restore_mode, names = "g", "alias-swap", ["orders"]

        def topology_for(self, n):
            return None
    s = MemStore()
    s.add("g/orders/orders-blue/full.backup")
    with pytest.raises(ops.OpError, match="already taken"):
        ops.seed_member(N(), s, G(), paths.DefaultPathLayout(), "orders")


def test_output_is_validated(use):
    use("Bad")
    with pytest.raises(naming.NamingError):
        naming.scheme().physical("orders", "t")
