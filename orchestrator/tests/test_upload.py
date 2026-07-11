"""ObjectStore.upload_file / upload_backups — the BACKUP_UPLOAD=pipeline leg (SSE-KMS on the
PUT that neo4j-admin can't send). No live S3."""

from neo4j_backup_core.clients import S3ObjectStore


class _FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploads.append((Filename, Bucket, Key, ExtraArgs))


def test_upload_file_passes_extra_args(monkeypatch, tmp_path):
    s = S3ObjectStore("b", sse="aws:kms", sse_kms_key_id="k")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    p = tmp_path / "x.backup"
    p.write_bytes(b"data")
    s.upload_file(str(p), "g/s/p/x.backup")
    _fn, bucket, key, extra = fake.uploads[0]
    assert bucket == "b" and key == "g/s/p/x.backup"
    assert extra == {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "k"}


def test_upload_file_extra_args_none_when_unset(monkeypatch, tmp_path):
    s = S3ObjectStore("b")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    p = tmp_path / "x.backup"
    p.write_bytes(b"d")
    s.upload_file(str(p), "k")
    assert fake.uploads[0][3] is None  # bucket default applies


class _FakeS3Full:
    """Fake supporting list (paginator), download, upload, delete — for aggregate/verify legs."""

    def __init__(self, contents):
        self.contents = contents
        self.uploaded, self.deleted = [], []

    def get_paginator(self, _name):
        contents = self.contents
        return type("_P", (), {"paginate": lambda self, **kw: [{"Contents": contents}]})()

    def download_file(self, Bucket, Key, Filename):
        with open(Filename, "wb") as f:
            f.write(b"x")

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploaded.append(Key)

    def delete_objects(self, Bucket, Delete):
        self.deleted += [o["Key"] for o in Delete["Objects"]]


def test_download_prefix(monkeypatch, tmp_path):
    s = S3ObjectStore("b")
    fake = _FakeS3Full([{"Key": "g/s/p/a.backup", "Size": 1, "LastModified": 0},
                        {"Key": "g/s/p/b.backup", "Size": 1, "LastModified": 1}])
    monkeypatch.setattr(s, "_client", lambda: fake)
    n = s.download_prefix("g/s/p/", str(tmp_path / "d"))
    assert n == 2
    assert (tmp_path / "d" / "a.backup").exists() and (tmp_path / "d" / "b.backup").exists()


def test_sync_up_uploads_new_deletes_collapsed_and_cleans(monkeypatch, tmp_path):
    # S3 has old full + a diff; local (post-aggregate) has only a new recovered full
    s = S3ObjectStore("b", sse="aws:kms")
    fake = _FakeS3Full([{"Key": "g/s/p/old-full.backup", "Size": 1, "LastModified": 0},
                        {"Key": "g/s/p/diff.backup", "Size": 1, "LastModified": 1}])
    monkeypatch.setattr(s, "_client", lambda: fake)
    d = tmp_path / "stage"
    d.mkdir()
    (d / "new-full.backup").write_bytes(b"1")
    latest = s.sync_up(str(d), "g/s/p/")
    assert fake.uploaded == ["g/s/p/new-full.backup"]
    assert set(fake.deleted) == {"g/s/p/old-full.backup", "g/s/p/diff.backup"}
    assert latest == "g/s/p/new-full.backup"
    assert not d.exists()


def test_upload_backups_uploads_backup_files_and_cleans(monkeypatch, tmp_path):
    s = S3ObjectStore("b", sse="aws:kms")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    d = tmp_path / "stage"
    d.mkdir()
    (d / "a.backup").write_bytes(b"1")
    (d / "b.backup").write_bytes(b"2")
    (d / "notes.txt").write_bytes(b"x")  # ignored
    latest = s.upload_backups(str(d), "g/s/p/")
    assert sorted(u[2] for u in fake.uploads) == ["g/s/p/a.backup", "g/s/p/b.backup"]
    assert latest == "g/s/p/b.backup"
    assert not d.exists()  # local staging removed
