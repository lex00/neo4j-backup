"""#74: after a seed the new database has no full to diff against. The diff lane must not fail
and must not run a full itself; it asks for a FULL in the full lane."""

import json
from datetime import datetime, timezone

import pytest

from neo4j_backup_core import artifacts, ops, paths
from test_artifacts import RUNNER, MemStore, admin, rec

LAYOUT = paths.DefaultPathLayout()
PHYS = "orders-20261115t100000"
FOLDER = LAYOUT.physical_prefix("g", "orders", PHYS)


class Neo:
    def __init__(self, created=datetime(2026, 11, 15, 10, 0, 0, 500000, tzinfo=timezone.utc)):
        self.created = created

    def resolve_physical(self, alias):
        return PHYS

    def database_created(self, name):
        return self.created


def put(store, file, full, when, dbid="id-1"):
    key = FOLDER + file
    store.add(key)
    meta = {**artifacts._normalise(rec(file, full, 1, 5, when, folder=FOLDER, dbid=dbid)), "key": key}
    store.put_text(artifacts.sidecar_key(key), json.dumps(meta))
    return key


def test_empty_folder_needs_a_full():
    assert ops.needs_full(Neo(), MemStore(), LAYOUT, "g", "orders") is True


def test_full_from_before_the_database_was_recreated_does_not_count():
    store = MemStore()
    put(store, "old-full.backup", True, "2026-11-15T09:00:00")  # the dropped store's full
    assert ops.needs_full(Neo(), store, LAYOUT, "g", "orders") is True


def test_full_of_the_current_store_counts():
    store = MemStore()
    put(store, "full.backup", True, "2026-11-15T10:00:00")  # same second as creation
    assert ops.needs_full(Neo(), store, LAYOUT, "g", "orders") is False


def test_backups_without_sidecars_are_a_possible_base():
    store = MemStore()
    store.add(FOLDER + "legacy.backup")
    assert ops.needs_full(Neo(), store, LAYOUT, "g", "orders") is False


def test_diff_with_no_base_is_skipped_not_failed():
    store = MemStore()
    calls, run = admin([])
    out = ops.backup_target(run, Neo(), store, RUNNER, LAYOUT, "g", "orders", "DIFF")
    assert out["skipped"] is True and out["needs_full"] is True and out["artifact"] is None
    assert calls == []  # neo4j-admin never ran


def test_diff_with_a_base_runs_and_reports_what_was_taken():
    store = MemStore()
    put(store, "1-full.backup", True, "2026-11-15T10:01:00")  # names sort by time, as neo4j-admin's do
    diff = rec("2-diff.backup", False, 6, 6, "2026-11-15T11:00:00", folder=FOLDER)

    def run(cmd):
        if "inspect" in cmd:
            return "neo4j 2026.05.0\n" + json.dumps([diff])
        store.add(FOLDER + "2-diff.backup")  # neo4j-admin wrote the artifact
        return ""
    out = ops.backup_target(run, Neo(), store, RUNNER, LAYOUT, "g", "orders", "DIFF")
    assert out["skipped"] is False and out["taken"] == "diff"


def test_auto_reports_the_full_it_fell_back_to():
    store = MemStore()
    full = rec("auto.backup", True, 1, 7, "2026-11-15T11:00:00", folder=FOLDER)

    def run(cmd):
        if "inspect" in cmd:
            return "neo4j 2026.05.0\n" + json.dumps([full])
        store.add(FOLDER + "auto.backup")
        return ""
    out = ops.backup_target(run, Neo(), store, RUNNER, LAYOUT, "g", "orders", "AUTO")
    assert out["taken"] == "full"


def test_dagster_diff_lane_requests_a_full_in_the_full_lane(monkeypatch):
    pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D

    store = MemStore()
    reqs = D.diff_lane_requests(["g/orders"], Neo(), store)
    assert reqs[0].tags["backup_kind"] == "full"
    assert reqs[0].run_config["ops"]["backup"]["config"]["kind"] == "FULL"

    put(store, "full.backup", True, "2026-11-15T10:05:00")
    reqs = D.diff_lane_requests(["g/orders"], Neo(), store)
    assert reqs[0].tags["backup_kind"] == "diff"
    assert reqs[0].run_config["ops"]["backup"]["config"]["kind"] == "DIFF"


def test_dagster_diff_lane_falls_back_to_diff_when_the_check_fails():
    pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D

    class Down(Neo):
        def resolve_physical(self, alias):
            raise RuntimeError("bolt down")
    reqs = D.diff_lane_requests(["g/orders"], Down(), MemStore())
    assert reqs[0].tags["backup_kind"] == "diff"


def test_tier_schedules_evaluate_with_several_members():
    # main's schedules returned several RunRequests with no run_key, which Dagster rejects on
    # every tick; evaluate both lanes of a tier with two members
    dg = pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D

    class Store(MemStore):
        pass
    for name in ("gold_full", "gold_diff"):
        sched = next(s for s in D.defs.schedules if s.name == name)
        with dg.instance_for_test() as inst:
            inst.add_dynamic_partitions(D.targets.name, ["demo/acme-orders", "demo/acme-graph"])
            ctx = dg.build_schedule_context(
                instance=inst, resources={"neo4j": Neo(), "store": Store()},
                repository_def=D.defs.get_repository_def(),
                scheduled_execution_time=datetime(2026, 11, 15, 12, tzinfo=timezone.utc))
            reqs = sched.evaluate_tick(ctx).run_requests
        assert len(reqs) == 2 and len({r.run_key for r in reqs}) == 2
        assert all(r.tags["backup_kind"] == "full" for r in reqs)  # empty store: every member needs a full


# --- #97: baseline full after a restore ---------------------------------------------------------

class G:
    id = "g"


def test_baseline_after_restore_gives_the_next_diff_a_base():
    store = MemStore()
    assert ops.needs_full(Neo(), store, LAYOUT, "g", "orders") is True  # just restored
    full = rec("1-full.backup", True, 1, 9, "2026-11-15T10:00:30", folder=FOLDER)
    taken = []

    def run(cmd):
        if "inspect" in cmd:
            return "neo4j 2026.05.0\n" + json.dumps([full])
        taken.append(cmd)
        store.add(FOLDER + "1-full.backup")
        return ""
    done = ops.baseline_after_restore(run, Neo(), store, RUNNER, LAYOUT, G(),
                                      [{"mode": "alias-swap", "alias": "orders", "newdb": PHYS}])
    assert done == [{"member": "orders", "physical": PHYS, "artifact": FOLDER + "1-full.backup",
                     "taken": "full"}]
    assert "FULL" in taken[0] and taken[0][-1] == PHYS
    assert ops.needs_full(Neo(), store, LAYOUT, "g", "orders") is False  # the diff lane can run


def test_baseline_failure_keeps_what_was_done():
    store = MemStore()

    def run(cmd):
        raise RuntimeError("runner down")
    with pytest.raises(ops.BaselineError) as e:
        ops.baseline_after_restore(run, Neo(), store, RUNNER, LAYOUT, G(),
                                   [{"mode": "by-name", "name": "orders"}])
    assert e.value.kind == "baseline_failed" and e.value.done == []
    assert "restore applied" in str(e.value)
