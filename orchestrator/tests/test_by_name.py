"""#48 by-name mode: policy validation + the restore branch (create-if-absent / gated replace).

The restore branch lives in `neo4j_backup_core.ops.restore_group` (shared by the Dagster/Airflow
adapters and the CLI); these tests drive it directly with fakes."""

import pytest

from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import DbGroup

LAYOUT = paths.DefaultPathLayout()


def _by_name(**kw) -> DbGroup:
    return DbGroup(id="g", restore_mode="by-name", databases=["foo", "bar"],
                   tier="gold", s3_prefix="s3://x/", **kw)


# --- policy validation ---------------------------------------------------------

def test_by_name_requires_databases():
    with pytest.raises(ValueError, match="needs 'databases'"):
        DbGroup(id="g", restore_mode="by-name", tier="gold", s3_prefix="s3://x/")


def test_by_name_rejects_aliases():
    with pytest.raises(ValueError, match="not 'aliases'"):
        DbGroup(id="g", restore_mode="by-name", databases=["foo"], aliases=["a"],
                tier="gold", s3_prefix="s3://x/")


def test_by_name_validates_database_names():
    # 'foo_bar' is a legal alias but NOT a legal database name (underscore)
    with pytest.raises(ValueError):
        DbGroup(id="g", restore_mode="by-name", databases=["foo_bar"], tier="gold", s3_prefix="s3://x/")


def test_alias_swap_rejects_databases():
    with pytest.raises(ValueError, match="only valid in by-name"):
        DbGroup(id="g", databases=["foo"], tier="gold", s3_prefix="s3://x/")  # default alias-swap


def test_names_accessor():
    assert _by_name().names == ["foo", "bar"]
    assert DbGroup(id="g", aliases=["a"], tier="gold", s3_prefix="s3://x/").names == ["a"]


# --- restore branch ------------------------------------------------------------

class _Store:
    def latest_artifact_key(self, prefix):
        return prefix + "art.backup"

    def uri(self, key):
        return "s3://b/" + key


class _Neo:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.dropped, self.seeded, self.altered = [], [], []

    def database_exists(self, n):
        return n in self.existing

    def drop_database(self, n):
        self.dropped.append(n)
        self.existing.discard(n)

    def seed_database(self, n, uri, **kw):
        self.seeded.append(n)

    def alter_alias(self, a, t):
        self.altered.append((a, t))


def test_by_name_create_if_absent():
    neo = _Neo()
    ops.restore_group(neo, _Store(), _by_name(), LAYOUT)
    assert neo.seeded == ["foo", "bar"]
    assert neo.dropped == [] and neo.altered == []   # nothing dropped, no alias swap


def test_by_name_existing_requires_replace_and_drops_nothing():
    neo = _Neo(existing=["foo"])
    with pytest.raises(ops.OpError, match="replace=true"):
        ops.restore_group(neo, _Store(), _by_name(), LAYOUT)
    assert neo.dropped == [] and neo.seeded == []    # pre-validation fails before any mutation


def test_by_name_replace_drops_then_seeds():
    neo = _Neo(existing=["foo"])
    ops.restore_group(neo, _Store(), _by_name(), LAYOUT, replace=True)
    assert neo.dropped == ["foo"]
    assert neo.seeded == ["foo", "bar"]
