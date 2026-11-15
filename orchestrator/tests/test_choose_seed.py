"""#114: with --until, restore picks the chain whose window covers the time, across physicals."""

import json

import pytest

from neo4j_backup_core import artifacts, ops, paths
from test_artifacts import MemStore, rec

LAYOUT = paths.DefaultPathLayout()
OLD = "g/orders/orders-1/"
NEW = "g/orders/orders-2/"


def put(store, folder, file, full, when, dbid):
    key = folder + file
    store.add(key)
    meta = {**artifacts._normalise(rec(file, full, 1, 5, when, folder=folder, dbid=dbid)),
            "key": key}
    store.put_text(artifacts.sidecar_key(key), json.dumps(meta))
    return key


def two_physicals():
    """orders-1: full 08:00, diffs 09:00 and 10:00. A restore at 11:00 makes orders-2: full 11:30,
    diff 12:00."""
    s = MemStore()
    put(s, OLD, "1-full.backup", True, "2026-11-15T08:00:00", "A")
    put(s, OLD, "2-diff.backup", False, "2026-11-15T09:00:00", "A")
    old_head = put(s, OLD, "3-diff.backup", False, "2026-11-15T10:00:00", "A")
    put(s, NEW, "4-full.backup", True, "2026-11-15T11:30:00", "B")
    new_head = put(s, NEW, "5-diff.backup", False, "2026-11-15T12:00:00", "B")
    return s, old_head, new_head


def test_time_in_the_older_physical_picks_its_chain():
    s, old_head, _ = two_physicals()
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T09:30:00Z")
    assert seed["key"] == old_head and seed["until"] == "2026-11-15T09:30:00+00:00"
    assert seed["chain"] == {"folder": OLD, "from": "2026-11-15T08:00:00",
                             "to": "2026-11-15T10:00:00"}


def test_time_in_the_newer_chain():
    s, _, new_head = two_physicals()
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T11:45:00")
    assert seed["key"] == new_head and seed["until"] and not seed["whole"]


def test_time_after_the_newest_backup_seeds_it_whole():
    s, _, new_head = two_physicals()
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T13:00:00Z")
    assert seed == {"key": new_head, "until": None, "whole": True,
                    "chain": {"folder": NEW, "from": "2026-11-15T11:30:00",
                              "to": "2026-11-15T12:00:00"}}


def test_full_only_member_seeds_whole_at_or_after_its_time():
    # the demo case from #77: a member that didn't change has only its full
    s = MemStore()
    key = put(s, NEW, "1-full.backup", True, "2026-11-15T11:30:00", "B")
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T11:45:00")
    assert seed["key"] == key and seed["until"] is None


@pytest.mark.parametrize("t", ["2026-11-15T07:00:00Z", "2026-11-15T10:30:00Z"])
def test_uncovered_time_fails_and_lists_windows(t):
    s, _, _ = two_physicals()
    with pytest.raises(ops.OpError, match="covers") as e:
        ops.choose_seed(s, LAYOUT, "g", "orders", t)
    assert "2026-11-15T08:00:00..2026-11-15T10:00:00" in str(e.value)


def test_without_until_the_newest_artifact():
    s, _, new_head = two_physicals()
    assert ops.choose_seed(s, LAYOUT, "g", "orders")["key"] == new_head


class Neo:
    def __init__(self):
        self.seeded, self.altered = [], []

    def alias_target(self, a):
        return "orders-2"

    def seed_database(self, name, uri, restore_until=None, **k):
        self.seeded.append((name, uri, restore_until))

    def privilege_commands(self, db):
        return []

    def alter_alias(self, a, t):
        self.altered.append((a, t))

    def database_exists(self, n):
        return False


class Group:
    id, restore_mode, names = "g", "alias-swap", ["orders", "customers"]

    def topology_for(self, n):
        return None


def test_group_restore_checks_every_member_before_seeding_any():
    s, old_head, _ = two_physicals()
    neo = Neo()  # 'customers' has no backups at all
    with pytest.raises(ops.OpError, match="customers"):
        ops.restore_group(neo, s, Group(), LAYOUT, restore_until="2026-11-15T09:30:00Z")
    assert neo.seeded == [] and neo.altered == []


def test_group_restore_seeds_the_older_chain_with_until():
    s, old_head, _ = two_physicals()
    put(s, "g/customers/customers-2/", "1-full.backup", True, "2026-11-15T08:00:00", "C")
    put(s, "g/customers/customers-2/", "2-diff.backup", False, "2026-11-15T12:00:00", "C")
    neo = Neo()
    out = ops.restore_group(neo, s, Group(), LAYOUT, restore_until="2026-11-15T09:30:00Z")
    assert neo.seeded[0][1] == f"s3://bucket/{old_head}"
    assert neo.seeded[0][2] == "2026-11-15T09:30:00+00:00"
    assert out["members"][0]["seed"]["chain"]["folder"] == OLD


def test_plan_shows_the_chosen_chain():
    s, _, _ = two_physicals()

    class One(Group):
        names = ["orders"]
    plan = ops.plan_restore(Neo(), s, One(), LAYOUT, until="2026-11-15T09:30:00Z")
    assert plan["members"][0]["seed"]["chain"]["folder"] == OLD
    assert plan["members"][0]["key"].startswith(OLD)


def test_empty_diffs_are_not_seeded_with_until():
    # a member that didn't change: full, then a diff with no transactions (tx 0-0)
    s = MemStore()
    full = put(s, NEW, "1-full.backup", True, "2026-11-15T11:00:00", "B")
    empty = rec("2-diff.backup", False, 0, 0, "2026-11-15T12:00:00", folder=NEW, dbid="B")
    s.add(NEW + "2-diff.backup")
    s.put_text(artifacts.sidecar_key(NEW + "2-diff.backup"),
               json.dumps({**artifacts._normalise(empty), "key": NEW + "2-diff.backup"}))
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T11:30:00Z")
    assert seed["key"] == full and seed["until"] is None


def test_until_after_the_last_real_diff_seeds_it_whole():
    s = MemStore()
    put(s, NEW, "1-full.backup", True, "2026-11-15T11:00:00", "B")
    d = put(s, NEW, "2-diff.backup", False, "2026-11-15T11:20:00", "B")
    empty = rec("3-diff.backup", False, 0, 0, "2026-11-15T12:00:00", folder=NEW, dbid="B")
    s.add(NEW + "3-diff.backup")
    s.put_text(artifacts.sidecar_key(NEW + "3-diff.backup"),
               json.dumps({**artifacts._normalise(empty), "key": NEW + "3-diff.backup"}))
    seed = ops.choose_seed(s, LAYOUT, "g", "orders", "2026-11-15T11:30:00Z")
    assert seed["key"] == d and seed["until"] is None
