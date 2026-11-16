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
