"""#156: load artifacts (.dump) live under <member>/_seed/ and never join a backup chain."""

from datetime import datetime, timezone

import pytest

from neo4j_backup_core import ops, paths
from neo4j_backup_core.clients import S3ObjectStore
from test_artifacts import MemStore

L = paths.DefaultPathLayout()


def test_load_prefix():
    assert L.load_prefix("g", "orders") == "g/orders/_seed/"
    assert paths.load_prefix("g", "orders") == "g/orders/_seed/"


def test_loads_are_listed_apart_from_chains():
    s = MemStore()
    s.add("g/orders/orders-1/full.backup")
    s.add("g/orders/_seed/load-2.dump")
    s.add("g/orders/_seed/load-1.backup")  # a load kept as .backup
    assert [k for k, _s, _m in s.list_artifacts("g/orders/")] == ["g/orders/orders-1/full.backup"]
    assert s.latest_artifact_key("g/orders/") == "g/orders/orders-1/full.backup"
    loads = ops.load_artifacts(s, L, "g", "orders")
    assert [(x["key"], x["kind"]) for x in loads] == [("g/orders/_seed/load-1.backup", "backup"),
                                                      ("g/orders/_seed/load-2.dump", "dump")]


class FakeS3:
    def get_paginator(self, _):
        return self

    def paginate(self, Bucket, Prefix):
        t = datetime(2026, 11, 15, tzinfo=timezone.utc)
        yield {"Contents": [{"Key": k, "Size": 1, "LastModified": t} for k in (
            "g/orders/orders-1/full.backup", "g/orders/_seed/x.dump",
            "g/orders/_seed/y.backup", "g/orders/orders-1/full.backup.meta.json")]}


def test_s3_backend(monkeypatch):
    st = S3ObjectStore("b")
    monkeypatch.setattr(st, "_client", lambda: FakeS3())
    assert [k for k, _s, _m in st.list_artifacts("g/orders/")] == ["g/orders/orders-1/full.backup"]
    assert [(k, kind) for k, _m, kind in st.list_load_artifacts("g/orders/")] == \
        [("g/orders/_seed/x.dump", "dump"), ("g/orders/_seed/y.backup", "backup")]


class _Blob:
    def __init__(self, name):
        self.name, self.size = name, 1
        self.last_modified = self.updated = datetime(2026, 11, 15, tzinfo=timezone.utc)


NAMES = ["g/orders/orders-1/full.backup", "g/orders/_seed/x.dump", "g/orders/_seed/y.backup"]


def test_azure_and_gcs_backends(monkeypatch):
    from neo4j_backup_core.clients import AzureObjectStore, GcsObjectStore

    class Cont:
        def list_blobs(self, name_starts_with=None, prefix=None):
            return [_Blob(n) for n in NAMES if n.startswith(name_starts_with or prefix)]
    for cls, attr in ((AzureObjectStore, "_c"), (GcsObjectStore, "_b")):
        st = cls.__new__(cls)
        monkeypatch.setattr(st, attr, lambda: Cont(), raising=False)
        assert [k for k, _s, _m in st.list_artifacts("g/orders/")] == [NAMES[0]], cls
        assert [k for k, _m, _kind in st.list_load_artifacts("g/orders/")] == NAMES[1:], cls


def test_import_to_dump_runs_import_then_dump_into_the_load_folder():
    from neo4j_backup_core.clients import BackupRunner
    calls = []
    out = ops.import_to_dump(lambda cmd: calls.append(cmd) or "", BackupRunner(), MemStore(), L,
                             "g", "orders", "loaddb", ["--nodes=/n.csv"])
    assert calls[0][:4] == ["neo4j-admin", "database", "import", "full"]
    assert calls[1][:3] == ["neo4j-admin", "database", "dump"] and calls[1][-1] == "loaddb"
    assert out["key"].startswith("g/orders/_seed/") and out["key"].endswith("/loaddb.dump")
    assert f"--to-path=s3://bucket/{out['key'][:-len('loaddb.dump')]}" in calls[1]


def test_export_command_is_one_shell_step():
    from neo4j_backup_core.clients import BackupRunner
    cmd = BackupRunner(exec_prefix=["docker", "exec", "r"]).export_command(
        "s3://b/g/o/p/d.backup", "export-o-1", "s3://b/g/o/_seed/1/", until="2026-11-16 05:12:46")
    assert cmd[:5] == ["docker", "exec", "r", "sh", "-c"]
    script = cmd[5]
    assert "database restore" in script and "'--restore-until=2026-11-16 05:12:46'" in script
    assert "database dump" in script and "rm -rf /data/databases/export-o-1" in script
    assert script.endswith("exit $rc")


def test_export_dumps_each_member_from_its_chain(monkeypatch):
    from neo4j_backup_core.clients import BackupRunner

    class Grp:
        id, names = "g", ["orders"]
    s = MemStore()
    s.add("g/orders/orders-1/full.backup")
    calls = []
    out = ops.export_dumps(lambda cmd: calls.append(cmd) or "", BackupRunner(), s, L, Grp())
    e = out["exports"][0]
    assert e["from"] == "g/orders/orders-1/full.backup" and e["key"].endswith(".dump")
    assert e["key"].startswith("g/orders/_seed/")
    assert "--restore-until" not in calls[0][-1]  # latest: no until
