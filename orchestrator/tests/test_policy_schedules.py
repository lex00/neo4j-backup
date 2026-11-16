"""#104: every scheduled job comes from the policy (Dagster half)."""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from neo4j_backup_core import ops, paths
from neo4j_backup_core.policy import Policy, load_policy

REPO = Path(__file__).resolve().parents[2]


def test_tier_maintenance_defaults():
    t = load_policy(REPO / "policies" / "demo.yaml").tiers["gold"]
    # no default aggregate (#219): it would collapse the chain and lose point-in-time restore
    assert (t.verify_cron, t.aggregate_cron, t.prune_cron, t.cleanup_cron) == \
        ("0 6 * * *", None, "0 7 * * 0", None)
    nightly = load_policy(REPO / "policies" / "demo.yaml").tiers["rebuild_nightly"]
    assert nightly.crons() == {"rebuild": "0 1 * * *"}  # rebuild-only: no maintenance by default


def test_dbms_block_defaults_and_validation():
    p = load_policy(REPO / "policies" / "demo.yaml")
    assert (p.dbms.metadata_cron, p.dbms.system_backup_cron, p.dbms.keep_metadata,
            p.dbms.keep_system) == ("0 2 * * *", "0 1 * * *", 14, 14)
    with pytest.raises(ValueError):
        Policy.model_validate({"db_groups": [], "tiers": {}, "dbms": {"keep_system": 0}})


def _schedules(policy_path):
    out = subprocess.run(
        [sys.executable, "-c", "import json; from neo4j_backup_dagster.definitions import defs; "
         "print(json.dumps({s.name: [s.cron_schedule, s.default_status.value] "
         "for s in defs.schedules}))"],
        env={**os.environ, "NEO4J_PASSWORD": "x", "NEO4J_BACKUP_POLICY": str(policy_path)},
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.splitlines()[-1])


def test_dagster_lists_every_policy_schedule():
    pytest.importorskip("dagster")
    s = _schedules(REPO / "policies" / "demo.yaml")
    for tier in ("gold", "silver", "bronze"):
        for lane in ("full", "diff", "verify", "prune"):
            assert f"{tier}_{lane}" in s
        assert f"{tier}_aggregate" not in s  # only when the tier sets aggregate_cron (#219)
    assert s["gold_verify"] == ["0 6 * * *", "STOPPED"]
    assert s["default_dbms_metadata"][0] == "0 2 * * *"
    assert s["default_dbms_system"][0] == "0 1 * * *"
    # the rebuild-only tier gets only its rebuild schedule (#108), no backups or maintenance
    assert [k for k in s if k.startswith("rebuild_nightly")] == ["rebuild_nightly_rebuild"]


def test_schedules_running_setting(tmp_path):
    pytest.importorskip("dagster")
    raw = yaml.safe_load((REPO / "policies" / "demo.yaml").read_text())
    raw["schedules"] = "running"
    raw["tiers"]["gold"]["verify_cron"] = "30 4 * * *"
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    s = _schedules(f)
    assert s["gold_verify"] == ["30 4 * * *", "RUNNING"]
    assert s["default_dbms_system"][1] == "RUNNING"


def test_maintenance_schedules_request_the_right_runs():
    dg = pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D
    when = datetime(2026, 11, 15, 6, tzinfo=timezone.utc)
    with dg.instance_for_test() as inst:
        inst.add_dynamic_partitions(D.targets.name, ["demo/acme-orders", "demo/acme-graph"])
        verify = next(s for s in D.defs.schedules if s.name == "gold_verify")
        ctx = dg.build_schedule_context(instance=inst, scheduled_execution_time=when,
                                        repository_def=D.defs.get_repository_def())
        reqs = verify.evaluate_tick(ctx).run_requests
        assert sorted(r.partition_key for r in reqs) == ["demo/acme-graph", "demo/acme-orders"]
        prune = next(s for s in D.defs.schedules if s.name == "gold_prune")
        req = prune.evaluate_tick(ctx).run_requests[0]
        assert req.run_config["ops"]["prune"]["config"] == {"tier": "gold", "dbms": False}


def test_tier_prune_leaves_the_dbms_sweep_alone():
    from test_artifacts import MemStore
    store = MemStore()
    for i in range(20):
        store.put_text(f"_dbms/metadata-{i:02d}.cypher", "x")
    pol = load_policy(REPO / "policies" / "demo.yaml")
    out = ops.prune(store, pol, paths.DefaultPathLayout(), groups=pol.groups_for_tier("gold"),
                    dbms=False)
    assert out["deleted"] == 0
    out = ops.prune(store, pol, paths.DefaultPathLayout(), groups=[])  # the DBMS sweep alone
    assert out["detail"] == {"_dbms/metadata": 6}  # keep_metadata 14 of 20


def test_scheduled_verify_and_aggregate_skip_members_without_backups():
    from test_artifacts import MemStore
    layout = paths.DefaultPathLayout()
    calls = []
    v = ops.verify_target(calls.append, MemStore(), None, layout, "g", "new", skip_missing=True)
    a = ops.aggregate_target(calls.append, MemStore(), None, layout, "g", "new", skip_missing=True)
    assert v["skipped"] and a["skipped"] and calls == []
    with pytest.raises(ops.OpError):  # by hand it still fails loudly
        ops.verify_target(calls.append, MemStore(), None, layout, "g", "new")
