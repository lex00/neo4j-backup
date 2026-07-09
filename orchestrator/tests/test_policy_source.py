"""#43 source-aware load_policy: file/s3, TTL cache, force bypass, last-known-good."""

import boto3
import pytest

from neo4j_backup_core import policy as P

MINI = """
db_groups:
  - id: g
    aliases: [a]
    tier: gold
    s3_prefix: s3://x/
tiers:
  gold: { full_cron: "0 2 * * *", diff_cron: "0 * * * *" }
"""


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    P._cache.clear()
    monkeypatch.setenv("POLICY_CACHE_TTL", "60")
    yield
    P._cache.clear()


def _counter(monkeypatch):
    calls = {"n": 0}

    def fake(_src):
        calls["n"] += 1
        return MINI

    monkeypatch.setattr(P, "_read_source", fake)
    return calls


def test_file_source_loads(tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text(MINI)
    assert P.load_policy(str(f)).group("g").aliases == ["a"]


def test_file_scheme_prefix(tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text(MINI)
    assert P.load_policy(f"file://{f}").group("g")


def test_cache_reads_once_within_ttl(monkeypatch):
    calls = _counter(monkeypatch)
    P.load_policy("x")
    P.load_policy("x")
    assert calls["n"] == 1


def test_ttl_zero_always_reads(monkeypatch):
    monkeypatch.setenv("POLICY_CACHE_TTL", "0")
    calls = _counter(monkeypatch)
    P.load_policy("x")
    P.load_policy("x")
    assert calls["n"] == 2


def test_force_bypasses_cache(monkeypatch):
    calls = _counter(monkeypatch)
    P.load_policy("x")
    P.load_policy("x", force=True)
    assert calls["n"] == 2


def test_last_known_good_on_failure(monkeypatch):
    state = {"fail": False}

    def fake(_src):
        if state["fail"]:
            raise RuntimeError("s3 down")
        return MINI

    monkeypatch.setattr(P, "_read_source", fake)
    good = P.load_policy("x")            # caches last-known-good
    state["fail"] = True
    assert P.load_policy("x", force=True) is good   # fetch fails -> serves last good


def test_cold_start_failure_raises(monkeypatch):
    def boom(_src):
        raise RuntimeError("down")

    monkeypatch.setattr(P, "_read_source", boom)
    with pytest.raises(RuntimeError):
        P.load_policy("x")


def test_s3_uri_parsed(monkeypatch):
    seen = {}

    class _Body:
        def read(self):
            return MINI.encode()

    class _Client:
        def get_object(self, Bucket, Key):
            seen.update(bucket=Bucket, key=Key)
            return {"Body": _Body()}

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _Client())
    pol = P.load_policy("s3://mybucket/dir/policy.yaml")
    assert seen == {"bucket": "mybucket", "key": "dir/policy.yaml"}
    assert pol.group("g")
