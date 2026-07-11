"""ObjectStore write_args — configurable SSE/extra args on the pipeline's PUT/COPY."""

from neo4j_backup_core.clients import S3ObjectStore


def test_write_args_assembly():
    s = S3ObjectStore("b", sse="aws:kms", sse_kms_key_id="key-1")
    assert s.write_args == {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "key-1"}


def test_write_args_json_escape_hatch_plus_sse():
    s = S3ObjectStore("b", write_args_json='{"BucketKeyEnabled": true}', sse="aws:kms")
    assert s.write_args["BucketKeyEnabled"] is True
    assert s.write_args["ServerSideEncryption"] == "aws:kms"


def test_write_args_default_empty():
    assert S3ObjectStore("b").write_args == {}


class _FakeS3:
    def __init__(self):
        self.put_kwargs = {}
        self.copy_kwargs = []

    def put_object(self, **kw):
        self.put_kwargs = kw

    def get_paginator(self, _name):
        page = {"Contents": [{"Key": "g/s/p/a.backup", "Size": 1, "LastModified": 0}]}
        return type("_P", (), {"paginate": lambda self, **kw: [page]})()

    def copy_object(self, **kw):
        self.copy_kwargs.append(kw)


def test_put_text_sends_write_args(monkeypatch):
    s = S3ObjectStore("b", sse="aws:kms", sse_kms_key_id="key-1")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    s.put_text("_dbms/x.cypher", "data")
    assert fake.put_kwargs["ServerSideEncryption"] == "aws:kms"
    assert fake.put_kwargs["SSEKMSKeyId"] == "key-1"
    assert fake.put_kwargs["Key"] == "_dbms/x.cypher"


def test_copy_prefix_sends_write_args(monkeypatch):
    s = S3ObjectStore("b", sse="aws:kms")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    n = s.copy_prefix("g/s/p/", "_verify/g/p/")
    assert n == 1
    assert fake.copy_kwargs[0]["ServerSideEncryption"] == "aws:kms"
    assert fake.copy_kwargs[0]["Key"] == "_verify/g/p/a.backup"


def test_no_write_args_when_unset(monkeypatch):
    s = S3ObjectStore("b")
    fake = _FakeS3()
    monkeypatch.setattr(s, "_client", lambda: fake)
    s.put_text("k", "v")
    assert "ServerSideEncryption" not in fake.put_kwargs  # bucket default applies
