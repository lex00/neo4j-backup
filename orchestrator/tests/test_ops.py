"""#58 P1 extraction — the shared op bodies in `neo4j_backup_core.ops`, exercised with fakes
(no live Neo4j, S3, or neo4j-admin). The adapters (Dagster/Airflow) and the CLI all route through
these, so this is where the backup/aggregate/verify/prune/restore behaviour is pinned."""

import pytest

from neo4j_backup_core import ops, paths
from neo4j_backup_core.clients import BackupRunner

LAYOUT = paths.DefaultPathLayout()
RUNNER = BackupRunner(scratch_path="/scratch")


class FakeStore:
    """Records the object-store calls ops makes; `arts` maps a prefix to a latest key."""

    def __init__(self, latest=None, artifacts=None, text_latest=None):
        self._latest = latest or {}
        self._artifacts = artifacts or {}
        self._text_latest = text_latest
        self.deleted, self.copied, self.synced, self.uploaded, self.downloaded = [], [], [], [], []
        self.text = {}

    def uri(self, prefix):
        return f"s3://bucket/{prefix}"

    def latest_artifact_key(self, prefix):
        return self._latest.get(prefix)

    def list_artifacts(self, prefix):
        return self._artifacts.get(prefix, [])

    def object_size(self, key):
        return 42

    def delete_keys(self, keys):
        keys = list(keys)
        self.deleted += keys
        return len(keys)

    def copy_prefix(self, src, scratch):
        self.copied.append((src, scratch))
        return 2

    def delete_prefix(self, scratch):
        self.deleted.append(scratch)

    def download_prefix(self, prefix, stage):
        self.downloaded.append((prefix, stage))
        import os
        os.makedirs(stage, exist_ok=True)
        open(os.path.join(stage, "full.backup"), "w").close()
        return 1

    def sync_up(self, stage, prefix):
        self.synced.append((stage, prefix))
        return f"{prefix}recovered.backup"

    def upload_backups(self, stage, prefix):
        self.uploaded.append((stage, prefix))
        return f"{prefix}uploaded.backup"

    def put_text(self, key, text):
        self.text[key] = text

    def get_text(self, key):
        return self.text.get(key, "// cypher")

    def latest_text_key(self, prefix):
        return self._text_latest

    def list_text_keys(self, prefix):
        return []  # no metadata exports by default


class FakeNeo4j:
    def __init__(self, physical="orders-20260101t000000", exists=False, target="old-phys"):
        self._physical, self._exists, self._target = physical, exists, target
        self.seeded, self.dropped, self.altered = [], [], []

    def resolve_physical(self, alias):
        return self._physical

    def database_exists(self, name):
        return self._exists

    def drop_database(self, name):
        self.dropped.append(name)

    def seed_database(self, name, uri, restore_until=None, topology=None, cypher_version=None):
        self.seeded.append({"name": name, "uri": uri, "until": restore_until})

    def alias_target(self, alias):
        return self._target

    def alter_alias(self, alias, target):
        self.altered.append((alias, target))


class FakeGroup:
    id = "demo"
    restore_mode = "alias-swap"

    def __init__(self, names, restore_mode="alias-swap"):
        self._names = names
        self.restore_mode = restore_mode

    @property
    def names(self):
        return self._names

    def topology_for(self, name):
        return None


def recorder():
    calls = []
    return calls, lambda cmd: calls.append(cmd)


# --- neo4j-admin legs -------------------------------------------------------------------------

def test_run_backup_admin_writes_direct_and_returns_key():
    store = FakeStore(latest={"demo/orders/phys/": "demo/orders/phys/full.backup"})
    calls, run = recorder()
    key = ops.run_backup(run, RUNNER, store, "phys", "demo/orders/phys/", "FULL")
    assert key == "demo/orders/phys/full.backup"
    assert calls[0][-1] == "phys" and "s3://bucket/demo/orders/phys/" in calls[0]


def test_run_backup_pipeline_stages_then_uploads():
    store = FakeStore()
    calls, run = recorder()
    key = ops.run_backup(run, RUNNER, store, "phys", "demo/orders/phys/", "AUTO",
                         upload="pipeline", staging="/tmp/ops-test-stage")
    assert key == "demo/orders/phys/uploaded.backup"
    assert store.uploaded == [("/tmp/ops-test-stage/_stage/phys", "demo/orders/phys/")]
    assert "/tmp/ops-test-stage/_stage/phys" in calls[0]  # neo4j-admin wrote to local stage


def test_run_verify_pipeline_cleans_up(tmp_path):
    store = FakeStore()
    calls, run = recorder()
    checked = ops.run_verify(run, RUNNER, store, "phys", "demo/orders/phys/", "_verify/demo/phys/",
                             upload="pipeline", staging=str(tmp_path))
    assert checked == 1
    assert len(calls) == 2  # aggregate + check
    assert not (tmp_path / "_verify" / "phys").exists()  # staged dir removed


def test_run_verify_admin_deletes_scratch_even_on_failure():
    store = FakeStore(latest={"_verify/demo/phys/": "_verify/demo/phys/full.backup"})
    def boom(_cmd):
        raise RuntimeError("check failed")
    with pytest.raises(RuntimeError):
        ops.run_verify(boom, RUNNER, store, "phys", "demo/orders/phys/", "_verify/demo/phys/")
    assert store.deleted == ["_verify/demo/phys/"]  # try/finally cleaned the copy


# --- target ops -------------------------------------------------------------------------------

def test_backup_target_resolves_physical():
    store = FakeStore(latest={"demo/orders/orders-20260101t000000/": "demo/orders/orders-20260101t000000/full.backup"})
    _calls, run = recorder()
    out = ops.backup_target(run, FakeNeo4j(), store, RUNNER, LAYOUT, "demo", "orders", "AUTO")
    assert out["physical"] == "orders-20260101t000000"
    assert out["artifact"].endswith("full.backup")


def test_backup_target_raises_when_no_physical():
    neo = FakeNeo4j(physical=None)
    with pytest.raises(ops.OpError, match="no physical database"):
        ops.backup_target(lambda c: None, neo, FakeStore(), RUNNER, LAYOUT, "demo", "orders", "AUTO")


def test_aggregate_target_raises_without_head():
    with pytest.raises(ops.OpError, match="no artifact"):
        ops.aggregate_target(lambda c: None, FakeStore(), RUNNER, LAYOUT, "demo", "orders")


def test_verify_target_checks_head_and_cleans_scratch():
    # exercises the store/runner argument order through verify_target -> run_verify (admin leg)
    store = FakeStore(latest={LAYOUT.alias_prefix("demo", "orders"):
                              f"{LAYOUT.physical_prefix('demo', 'orders', 'orders-x')}full.backup"})
    _calls, run = recorder()
    out = ops.verify_target(run, store, RUNNER, LAYOUT, "demo", "orders")
    assert out["physical"] == "orders-x" and out["consistent"] is True
    assert store.deleted == ["_verify/demo/orders-x/"]  # scratch copy cleaned


# --- prune ------------------------------------------------------------------------------------

def _prune_fixture():
    from datetime import datetime, timezone
    old = datetime(2000, 1, 1, tzinfo=timezone.utc)
    new = datetime(2999, 1, 1, tzinfo=timezone.utc)
    store = FakeStore(artifacts={"demo/orders/": [
        ("demo/orders/p/old.backup", 1, old),
        ("demo/orders/p/head.backup", 1, new),  # newest chain head -> always kept
    ]})

    class G:
        id, retention_days = "demo", 7
        names = ["orders"]

    class Pol:
        db_groups = [G()]
    return store, Pol()


def test_prune_keeps_head_and_deletes_stale():
    store, pol = _prune_fixture()
    out = ops.prune(store, pol, LAYOUT)
    assert "demo/orders/p/old.backup" in store.deleted
    assert "demo/orders/p/head.backup" not in store.deleted
    assert out["deleted"] >= 1 and out["dry_run"] is False


def test_prune_dry_run_enumerates_without_deleting():
    store, pol = _prune_fixture()
    out = ops.prune(store, pol, LAYOUT, dry_run=True)
    assert out["dry_run"] is True
    assert "demo/orders/p/old.backup" in out["keys"]  # blast radius reported
    assert store.deleted == []                          # but nothing actually removed


def test_plan_restore_reports_drops_without_mutating():
    store = FakeStore(latest=dict([_head("orders")]))
    neo = FakeNeo4j(exists=True)
    plan = ops.plan_restore(neo, store, FakeGroup(["orders"], restore_mode="by-name"),
                            LAYOUT, replace=True)
    assert plan["drops"] == ["orders"] and plan["members"][0]["action"] == "drop+recreate"
    assert neo.dropped == [] and neo.seeded == []  # planning is inert


# --- restore ----------------------------------------------------------------------------------

def _head(name):  # the fake latest-key for a member, via the real layout (slug adds a suffix)
    return LAYOUT.alias_prefix("demo", name), f"{LAYOUT.alias_prefix('demo', name)}full.backup"


def test_restore_group_alias_swap_seeds_then_cuts_over():
    store = FakeStore(latest=dict([_head("orders"), _head("customers")]))
    neo = FakeNeo4j()
    out = ops.restore_group(neo, store, FakeGroup(["orders", "customers"]), LAYOUT)
    assert out["mode"] == "alias-swap"
    assert len(neo.seeded) == 2
    assert [a for a, _ in neo.altered] == ["orders", "customers"]  # cut over after seeding


def test_restore_group_by_name_prevalidates_before_any_drop():
    # 'orders' exists (replaceable), 'customers' has no artifact -> must fail BEFORE dropping 'orders'
    store = FakeStore(latest=dict([_head("orders")]))  # 'customers' missing
    neo = FakeNeo4j(exists=True)
    group = FakeGroup(["orders", "customers"], restore_mode="by-name")
    with pytest.raises(ops.OpError, match="no artifact for demo/customers"):
        ops.restore_group(neo, store, group, LAYOUT, replace=True)
    assert neo.dropped == []  # nothing destroyed — precondition checked up front


def test_restore_group_by_name_refuses_existing_without_replace():
    store = FakeStore(latest=dict([_head("orders")]))
    neo = FakeNeo4j(exists=True)
    group = FakeGroup(["orders"], restore_mode="by-name")
    with pytest.raises(ops.OpError, match="set replace=true"):
        ops.restore_group(neo, store, group, LAYOUT)
    assert neo.dropped == []


def test_restore_group_by_name_replaces_then_seeds():
    store = FakeStore(latest=dict([_head("orders")]))
    neo = FakeNeo4j(exists=True)
    group = FakeGroup(["orders"], restore_mode="by-name")
    out = ops.restore_group(neo, store, group, LAYOUT, replace=True)
    assert neo.dropped == ["orders"] and len(neo.seeded) == 1
    assert out["members"][0]["replaced"] is True


# --- metadata + system ------------------------------------------------------------------------

def test_export_metadata_puts_text(monkeypatch):
    monkeypatch.setattr(ops.metadata, "capture", lambda neo: {})
    monkeypatch.setattr(ops.metadata, "render", lambda cap, ts: "// export")
    store = FakeStore()
    out = ops.export_metadata(FakeNeo4j(), store, LAYOUT)
    assert out["key"].startswith("_dbms/metadata-") and out["bytes"] == len("// export")
    assert store.text[out["key"]] == "// export"


def test_restore_metadata_raises_when_empty():
    with pytest.raises(ops.OpError, match="no metadata artifact"):
        ops.restore_metadata(FakeNeo4j(), FakeStore(text_latest=None), LAYOUT)


def test_system_backup_targets_reserved_prefix():
    store = FakeStore(latest={"_dbms/system/": "_dbms/system/full.backup"})
    _calls, run = recorder()
    out = ops.system_backup(run, store, RUNNER, LAYOUT)
    assert out["key"] == "_dbms/system/full.backup"


# --- bulk import (#16) ------------------------------------------------------------------------

def test_import_command_structures_call_and_passes_args_through():
    argv = RUNNER.import_command("orders-x", ["--nodes=/n.csv", "--relationships=/r.csv"])
    # database FIRST (multi-value --nodes would swallow a trailing database), then passthrough args
    assert argv == ["neo4j-admin", "database", "import", "full",
                    "orders-x", "--nodes=/n.csv", "--relationships=/r.csv"]


def test_import_database_runs_the_built_command():
    calls, run = recorder()
    out = ops.import_database(run, RUNNER, "orders-x", ["--nodes=/n.csv"])
    assert calls == [RUNNER.import_command("orders-x", ["--nodes=/n.csv"])]
    assert out == {"database": "orders-x", "argv": calls[0]}
