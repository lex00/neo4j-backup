"""#223, #224, #228: Dagster run tags, the group lock, empty ticks, the sensor's status."""

from datetime import datetime, timezone

import pytest

dg = pytest.importorskip("dagster")

from neo4j_backup_core import locks  # noqa: E402
from neo4j_backup_dagster import definitions as D  # noqa: E402
from test_artifacts import MemStore  # noqa: E402


def _tick(name, partitions):
    sched = next(s for s in D.defs.schedules if s.name == name)
    with dg.instance_for_test() as inst:
        if partitions:
            inst.add_dynamic_partitions(D.targets.name, partitions)
        ctx = dg.build_schedule_context(
            instance=inst, repository_def=D.defs.get_repository_def(),
            scheduled_execution_time=datetime(2026, 11, 16, 6, tzinfo=timezone.utc))
        return sched.evaluate_tick(ctx)


def test_verify_runs_carry_the_group_and_the_full_lane():
    reqs = _tick("gold_verify", ["demo/acme-orders"]).run_requests
    assert reqs[0].tags["neo4j_group"] == "demo" and reqs[0].tags["backup_kind"] == "full"


def test_a_tick_with_no_partitions_says_why():
    out = _tick("gold_verify", [])
    assert not out.run_requests and "no partitions" in (out.skip_message or "")


def test_sensor_default_follows_the_policy():
    # demo.yaml leaves schedules stopped, so the sensor is stopped too
    assert D.reconcile_registry.default_status == dg.DefaultSensorStatus.STOPPED


def test_restore_waits_only_in_its_baseline_step():
    assert "backup_kind" not in (D.restore_group.tags or {})
    assert "backup_kind" not in (D.move_job.tags or {})


class LockStore(MemStore):
    def create_exclusive(self, key, text):
        if key in self.objects:
            return False
        self.objects[key] = text
        return True


def test_dagster_ops_take_the_group_lock(monkeypatch):
    monkeypatch.setattr(D, "LOCK_WAIT", 0)
    s = LockStore()
    held = locks.acquire(s, "demo", "a rebuild")
    with pytest.raises(locks.Locked):
        with D._group_lock(s, "demo", "backup acme-orders"):
            pass
    locks.release(s, "demo", held)
    with D._group_lock(s, "demo", "backup acme-orders"):
        pass


def test_restore_dry_run_logs_the_plan_and_changes_nothing(monkeypatch):
    """#249: Dagster's restore_group takes dry_run, like the CLI."""
    from neo4j_backup_core import ops
    from neo4j_backup_dagster.resources import Neo4jResource, ObjectStoreResource, RunnerResource
    monkeypatch.setattr(ops, "plan_restore", lambda *a, **k: {"point": {"aligned": True}})

    def boom(*a, **k):
        raise AssertionError("restore ran during a dry run")
    monkeypatch.setattr(ops, "restore_group", boom)
    monkeypatch.setattr(ops, "baseline_after_restore", boom)
    res = D.restore_group.execute_in_process(
        run_config={"ops": {"restore_group_op": {"config": {"group_id": "demo",
                                                            "dry_run": True}}}},
        resources={"neo4j": Neo4jResource(uri="neo4j://x:7687", password="p"),
                   "store": ObjectStoreResource(bucket="b"), "runner": RunnerResource(),
                   "pipes_subprocess_client": dg.PipesSubprocessClient()})
    assert res.success
    out = res.output_for_node("restore_group_op")
    assert out["dry_run"] and out["plan"] == {"point": {"aligned": True}}
