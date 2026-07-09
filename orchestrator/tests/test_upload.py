"""ObjectStore.upload_file / upload_backups — the BACKUP_UPLOAD=pipeline leg (SSE-KMS on the
PUT that neo4j-admin can't send). No live S3."""

from neo4j_backup_core.clients import ObjectStore


class _FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploads.append((Filename, Bucket, Key, ExtraArgs))


def test_upload_file_passes_extra_args(monkeypatch, tmp_path):
    s = ObjectStore("b", sse="aws:kms", sse_kms_key_id="k")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    p = tmp_path / "x.backup"
    p.write_bytes(b"data")
    s.upload_file(str(p), "g/s/p/x.backup")
    _fn, bucket, key, extra = fake.uploads[0]
    assert bucket == "b" and key == "g/s/p/x.backup"
    assert extra == {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "k"}


def test_upload_file_extra_args_none_when_unset(monkeypatch, tmp_path):
    s = ObjectStore("b")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    p = tmp_path / "x.backup"
    p.write_bytes(b"d")
    s.upload_file(str(p), "k")
    assert fake.uploads[0][3] is None  # bucket default applies


def test_upload_backups_uploads_backup_files_and_cleans(monkeypatch, tmp_path):
    s = ObjectStore("b", sse="aws:kms")
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
