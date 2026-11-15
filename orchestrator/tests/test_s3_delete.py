"""#76: S3 DeleteObjects takes at most 1000 keys and reports per-key errors in its response."""

import pytest

from neo4j_backup_core.clients import DeleteError, S3ObjectStore


class FakeS3:
    def __init__(self, fail=()):
        self.batches, self.fail = [], set(fail)

    def delete_objects(self, Bucket, Delete):
        keys = [o["Key"] for o in Delete["Objects"]]
        assert len(keys) <= 1000, "S3 rejects more than 1000 keys per request"
        self.batches.append(len(keys))
        return {"Errors": [{"Key": k, "Code": "AccessDenied", "Message": "nope"}
                           for k in keys if k in self.fail]}


def _store(fake, monkeypatch):
    s = S3ObjectStore("b")
    monkeypatch.setattr(s, "_client", lambda: fake)
    return s


def test_2500_keys_go_in_three_batches(monkeypatch):
    fake = FakeS3()
    assert _store(fake, monkeypatch).delete_keys([f"k{i}" for i in range(2500)]) == 2500
    assert fake.batches == [1000, 1000, 500]


def test_per_key_errors_raise_after_every_batch(monkeypatch):
    fake = FakeS3(fail={"k3", "k1500"})
    with pytest.raises(DeleteError) as e:
        _store(fake, monkeypatch).delete_keys([f"k{i}" for i in range(2500)])
    assert fake.batches == [1000, 1000, 500]  # kept going after the first batch's error
    assert e.value.deleted == 2498
    assert [k for k, _c, _m in e.value.failed] == ["k3", "k1500"]
    assert "AccessDenied" in str(e.value)


def test_no_keys_no_request(monkeypatch):
    fake = FakeS3()
    assert _store(fake, monkeypatch).delete_keys([]) == 0 and fake.batches == []
