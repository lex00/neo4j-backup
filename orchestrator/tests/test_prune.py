"""#73: chain-aware prune. Full/diff comes from the #118 sidecars; whole chains are deleted only
when nothing kept depends on them."""

import json
from datetime import datetime, timedelta, timezone

from neo4j_backup_core import artifacts, ops, paths
from test_artifacts import MemStore, admin, rec

LAYOUT = paths.DefaultPathLayout()
NOW = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)


def day(n, hour=0):  # n days before NOW, as neo4j-admin prints it (UTC, no offset)
    return (NOW - timedelta(days=n) + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M:%S")


class G:
    def __init__(self, gid="g", names=("orders",), retention_days=7):
        self.id, self.names, self.retention_days = gid, list(names), retention_days


class Pol:
    def __init__(self, *groups):
        self.db_groups = list(groups) or [G()]


def put(store, folder, file, full, lo, hi, when, dbid="id-1"):
    key = folder + file
    store.add(key)
    meta = {**artifacts._normalise(rec(file, full, lo, hi, when, folder=folder, dbid=dbid)),
            "key": key}
    store.put_text(artifacts.sidecar_key(key), json.dumps(meta))
    return key


P1 = "g/orders/orders-1/"


def weekly_chain(store):
    """The issue's repro: a full 8 days old, then a diff every day since."""
    put(store, P1, "full.backup", True, 1, 10, day(8))
    return [put(store, P1, f"diff{i}.backup", False, 10 + i, 10 + i, day(8 - i)) for i in range(1, 9)]


def test_issue_repro_keeps_the_full_the_head_needs():
    store = MemStore()
    weekly_chain(store)
    out = ops.prune(store, Pol(), LAYOUT, dry_run=True, now=NOW)
    assert out["keys"] == []                 # nothing the head needs
    assert out["kept_for_chains"] == 1       # the full is past retention but needed
    assert store.latest_artifact_key(P1) in store.objects


def test_old_chain_is_deleted_whole_once_a_newer_chain_covers_the_cutoff():
    store = MemStore()
    old = [put(store, P1, "a-full.backup", True, 1, 5, day(20)),
           put(store, P1, "a-diff.backup", False, 6, 6, day(19))]
    keep = [put(store, P1, "b-full.backup", True, 1, 9, day(10)),     # covers the cutoff (day 7)
            put(store, P1, "b-diff.backup", False, 10, 10, day(6)),
            put(store, P1, "c-full.backup", True, 1, 12, day(1))]
    out = ops.prune(store, Pol(), LAYOUT, now=NOW)
    assert sorted(out["keys"]) == sorted(old)
    assert all(k in store.objects for k in keep)
    assert not any(artifacts.sidecar_key(k) in store.objects for k in old)  # sidecars went too
    assert out["sidecars_deleted"] == 2


def test_old_physicals_are_pruned_across_restores():
    # before a restore the member lived in orders-1; its last chain there is past retention and
    # a newer full in orders-2 covers the cutoff, so orders-1 goes entirely
    store = MemStore()
    gone = [put(store, P1, "full.backup", True, 1, 5, day(30)),
            put(store, P1, "diff.backup", False, 6, 6, day(29))]
    put(store, "g/orders/orders-2/", "full.backup", True, 1, 5, day(9), dbid="id-2")
    put(store, "g/orders/orders-2/", "diff.backup", False, 6, 6, day(2), dbid="id-2")
    out = ops.prune(store, Pol(), LAYOUT, now=NOW)
    assert sorted(out["keys"]) == sorted(gone)


def test_newest_chain_is_always_kept():
    store = MemStore()
    head = [put(store, P1, "full.backup", True, 1, 5, day(40))]
    assert ops.prune(store, Pol(), LAYOUT, now=NOW)["keys"] == []
    assert head[0] in store.objects


def test_artifacts_without_sidecars_are_kept_and_reported():
    store = MemStore()
    store.add(P1 + "legacy.backup")  # taken before #118
    put(store, P1, "full.backup", True, 1, 5, day(1))
    out = ops.prune(store, Pol(), LAYOUT, now=NOW)
    assert out["keys"] == [] and out["unknown_kept"] == 1


def test_missing_sidecars_are_backfilled_when_neo4j_admin_can_run():
    store = MemStore()
    store.add(P1 + "legacy.backup")
    put(store, P1, "full.backup", True, 1, 5, day(10))  # covers the cutoff, so legacy can go
    put(store, P1, "full2.backup", True, 1, 6, day(1))
    legacy = [rec("legacy.backup", True, 1, 3, day(30), folder=P1)]
    calls, run = admin(legacy)
    out = ops.prune(store, Pol(), LAYOUT, dry_run=True, run_admin=run,
                    runner=__import__("test_artifacts").RUNNER, now=NOW)
    assert out["keys"] == [P1 + "legacy.backup"] and out["unknown_kept"] == 0
    assert P1 + "legacy.backup" in store.objects  # dry run: listed, not deleted


def test_two_stores_in_one_folder_prune_independently():
    # #120: a by-name --replace puts a second store's chain in the same folder
    store = MemStore()
    gone = [put(store, P1, "a-full.backup", True, 1, 5, day(20), dbid="A"),
            put(store, P1, "a-diff.backup", False, 6, 6, day(18), dbid="A")]
    put(store, P1, "b-full.backup", True, 1, 7, day(9), dbid="B")
    put(store, P1, "b-diff.backup", False, 8, 8, day(1), dbid="B")
    assert sorted(ops.prune(store, Pol(), LAYOUT, now=NOW)["keys"]) == sorted(gone)
