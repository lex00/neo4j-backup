"""#52 AzureObjectStore — Azure Blob backend behaviour, and that the base composites run over
its primitives. Uses a fake container client (no live Azurite / no network)."""

import types

import pytest

pytest.importorskip("azure.storage.blob")  # skip where the azure extra isn't installed

from neo4j_backup_core.clients import AzureObjectStore


class _FakeContainer:
    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.uploaded, self.deleted, self.copied = [], [], []
        self.read_whole = []
        self.copy_states, self.copy_args = {}, []

    class _BlobClient:
        def __init__(self, c, key):
            self._c, self.key = c, key

        @property
        def url(self):
            return f"az://{self.key}"

        def get_blob_properties(self):
            states = self._c.copy_states.get(self.key)
            status = states.pop(0) if states else None
            return types.SimpleNamespace(size=len(self._c.blobs.get(self.key, b"")),
                                         copy=types.SimpleNamespace(status=status,
                                                                    status_description="x"))

        def download_blob(self, **kw):
            data = self._c.blobs[self.key]
            c = self._c

            def readall():
                c.read_whole.append(self.key)
                return data
            return types.SimpleNamespace(readall=readall, readinto=lambda f: f.write(data))

        def start_copy_from_url(self, url, **kw):
            src = url.split("az://", 1)[1]
            self._c.blobs[self.key] = self._c.blobs[src]
            self._c.copied.append((src, self.key))
            self._c.copy_args.append(kw)

    def list_blobs(self, name_starts_with=""):
        return [types.SimpleNamespace(name=n, size=len(b), last_modified=i)
                for i, (n, b) in enumerate(self.blobs.items()) if n.startswith(name_starts_with)]

    def get_blob_client(self, key):
        return _FakeContainer._BlobClient(self, key)

    def delete_blob(self, key):
        self.blobs.pop(key)
        self.deleted.append(key)

    def upload_blob(self, name, data, overwrite=True, **kw):
        self.blobs[name] = data.read() if hasattr(data, "read") else data
        self.uploaded.append((name, kw))


@pytest.fixture
def store(monkeypatch):
    s = AzureObjectStore("mycontainer")
    fake = _FakeContainer()
    monkeypatch.setattr(s, "_c", lambda **kw: fake)
    s._fake = fake
    return s


def test_uri_is_azb(store, monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
    assert store.uri("g/s/p/x.backup") == "azb://acct/mycontainer/g/s/p/x.backup"


def test_put_get_text_round_trip(store):
    store.put_text("_dbms/m.cypher", "hello")
    assert store.get_text("_dbms/m.cypher") == "hello"


def test_list_artifacts_filters_backup(store):
    store._fake.blobs = {"g/a.backup": b"1", "g/notes.txt": b"2", "g/b.backup": b"3"}
    assert sorted(k for k, _s, _m in store.list_artifacts("g/")) == ["g/a.backup", "g/b.backup"]


def test_upload_and_delete(store, tmp_path):
    p = tmp_path / "x.backup"
    p.write_bytes(b"data")
    store.upload_file(str(p), "g/x.backup")
    assert store._fake.blobs["g/x.backup"] == b"data"
    assert store.delete_keys(["g/x.backup"]) == 1
    assert "g/x.backup" not in store._fake.blobs


def test_base_composites_run_over_azure(store, tmp_path):
    # upload_backups + sync_up (base) drive Azure upload_file / list / delete
    d = tmp_path / "stage"
    d.mkdir()
    (d / "a.backup").write_bytes(b"1")
    (d / "b.backup").write_bytes(b"2")
    latest = store.upload_backups(str(d), "g/s/p/", cleanup=True)
    assert latest == "g/s/p/b.backup"
    assert set(store._fake.blobs) == {"g/s/p/a.backup", "g/s/p/b.backup"}
    assert not d.exists()


def test_download_streams_to_disk(store, tmp_path):
    """#79: a backup is streamed into the file, never read whole into memory."""
    store._fake.blobs["g/a/p/x.backup"] = b"0123456789" * 1000
    dst = tmp_path / "x.backup"
    store._download("g/a/p/x.backup", str(dst))
    assert dst.read_bytes() == b"0123456789" * 1000
    assert store._fake.read_whole == []


def test_copy_waits_for_pending_then_success(store):
    """#80: the copy is only done when Azure says so."""
    store._fake.blobs["a"] = b"x"
    store._fake.copy_states["b"] = ["pending", "pending", "success"]
    naps = []
    store._copy("a", "b", sleep=naps.append)
    assert len(naps) == 2 and store._fake.copy_states["b"] == []


def test_copy_failed_raises_with_the_names(store):
    store._fake.blobs["a"] = b"x"
    store._fake.copy_states["b"] = ["pending", "failed"]
    with pytest.raises(RuntimeError, match="a -> b failed"):
        store._copy("a", "b", sleep=lambda s: None)


def test_copy_times_out(store, monkeypatch):
    monkeypatch.setenv("AZURE_COPY_TIMEOUT", "0")
    store._fake.blobs["a"] = b"x"
    store._fake.copy_states["b"] = ["pending"] * 5
    with pytest.raises(TimeoutError, match="still pending"):
        store._copy("a", "b", sleep=lambda s: None)


def test_copy_carries_the_encryption_scope(store):
    store.write_args = {"encryption_scope": "scope1", "overwrite_conditions": "ignored"}
    store._fake.blobs["a"] = b"x"
    store._copy("a", "b")
    assert store._fake.copy_args == [{"encryption_scope": "scope1"}]
