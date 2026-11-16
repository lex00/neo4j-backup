"""#52 GcsObjectStore — GCS backend behaviour + base composites over its primitives. Fake
bucket, no live fake-gcs / no network."""

import types

import pytest

pytest.importorskip("google.cloud.storage")  # skip where the gcp extra isn't installed

from neo4j_backup_core.clients import GcsObjectStore


class _FakeBucket:
    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.deleted, self.copied, self.written = [], [], []

    class _Blob:
        def __init__(self, b, key, **kw):
            self._b, self.name, self.kw = b, key, kw
            self.metadata = self.content_type = None

        @property
        def size(self):
            return len(self._b.blobs[self.name])

        @property
        def updated(self):
            return list(self._b.blobs).index(self.name)

        def upload_from_filename(self, path, content_type=None):
            with open(path, "rb") as f:
                self._b.blobs[self.name] = f.read()
            self._b.written.append(self)

        def upload_from_string(self, text, content_type=None):
            self._b.blobs[self.name] = text.encode() if isinstance(text, str) else text
            self._b.written.append(self)

        def download_as_bytes(self):
            return self._b.blobs[self.name]

        def download_to_filename(self, path):
            with open(path, "wb") as f:
                f.write(self._b.blobs[self.name])

        def delete(self):
            self._b.blobs.pop(self.name)
            self._b.deleted.append(self.name)

    def blob(self, key, **kw):
        return _FakeBucket._Blob(self, key, **kw)

    def get_blob(self, key):
        return _FakeBucket._Blob(self, key)

    def list_blobs(self, prefix=""):
        return [_FakeBucket._Blob(self, n) for n in list(self.blobs) if n.startswith(prefix)]

    def copy_blob(self, src_blob, _dst_bucket, dst_name):
        self.blobs[dst_name] = self.blobs[src_blob.name]
        self.copied.append((src_blob.name, dst_name))


@pytest.fixture
def store(monkeypatch):
    s = GcsObjectStore("mybucket")
    fake = _FakeBucket()
    monkeypatch.setattr(s, "_b", lambda: fake)
    s._fake = fake
    return s


def test_uri_is_gs(store):
    assert store.uri("g/s/p/x.backup") == "gs://mybucket/g/s/p/x.backup"


def test_put_get_text_round_trip(store):
    store.put_text("_dbms/m.cypher", "hello")
    assert store.get_text("_dbms/m.cypher") == "hello"


def test_list_artifacts_filters_backup(store):
    store._fake.blobs = {"g/a.backup": b"1", "g/notes.txt": b"2", "g/b.backup": b"3"}
    assert sorted(k for k, _s, _m in store.list_artifacts("g/")) == ["g/a.backup", "g/b.backup"]


def test_base_composites_run_over_gcs(store, tmp_path):
    d = tmp_path / "stage"
    d.mkdir()
    (d / "a.backup").write_bytes(b"1")
    (d / "b.backup").write_bytes(b"2")
    latest = store.upload_backups(str(d), "g/s/p/", cleanup=True)
    assert latest == "g/s/p/b.backup"
    assert set(store._fake.blobs) == {"g/s/p/a.backup", "g/s/p/b.backup"}
    assert not d.exists()
    # copy_prefix + sync_up (collapse) over GCS primitives
    store.copy_prefix("g/s/p/", "_verify/x/")
    assert store._fake.copied
    s2 = tmp_path / "agg"
    s2.mkdir()
    (s2 / "recovered.backup").write_bytes(b"R")
    store.sync_up(str(s2), "g/s/p/")
    assert sorted(k for k, _s, _m in store.list_artifacts("g/s/p/")) == ["g/s/p/recovered.backup"]


def test_write_args_reach_the_blob(monkeypatch, tmp_path):
    """#81: CMEK and the other write_args are applied, not dropped."""
    s = GcsObjectStore("b", write_args_json='{"kms_key_name": "projects/p/keys/k", '
                                           '"metadata": {"owner": "backup"}}')
    fake = _FakeBucket()
    monkeypatch.setattr(s, "_b", lambda: fake)
    f = tmp_path / "x.backup"
    f.write_bytes(b"x")
    s.upload_file(str(f), "g/a/p/x.backup")
    s.put_text("g/a/p/x.backup.meta.json", "{}")
    assert [w.kw for w in fake.written] == [{"kms_key_name": "projects/p/keys/k"}] * 2
    assert all(w.metadata == {"owner": "backup"} for w in fake.written)
    assert fake.written[1].content_type == "text/plain; charset=utf-8"


def test_unsupported_write_args_are_refused():
    with pytest.raises(ValueError, match="ServerSideEncryption"):
        GcsObjectStore("b", write_args_json='{"ServerSideEncryption": "aws:kms"}')
