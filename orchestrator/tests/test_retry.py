"""#25 classification + #19 retry loop — no live cluster needed."""

import time

import pytest
from neo4j.exceptions import (
    ClientError,
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

from neo4j_backup_core import retry


def _coded(code: str) -> Neo4jError:
    """A real driver error carrying `code` (its class is chosen by the driver from the code)."""
    return Neo4jError._hydrate_neo4j(code=code, message="test")


# --- #25 classification ----------------------------------------------------------

def test_retryable_by_class():
    assert retry.is_retryable(ServiceUnavailable("x"))
    assert retry.is_retryable(SessionExpired("x"))
    assert retry.is_retryable(TransientError("x"))


def test_retryable_by_code_leader_reelection():
    e = _coded("Neo.ClientError.Cluster.NotALeader")
    assert retry.is_retryable(e)
    assert not retry.is_auth_expired(e)


def test_auth_expired_is_retryable_and_flagged():
    for code in ("Neo.ClientError.Security.TokenExpired",
                 "Neo.ClientError.Security.AuthorizationExpired"):
        e = _coded(code)
        assert retry.is_retryable(e)
        assert retry.is_auth_expired(e)


def test_not_retryable():
    # a genuine client error (bad Cypher) must NOT be retried
    assert not retry.is_retryable(_coded("Neo.ClientError.Statement.SyntaxError"))
    # a non-Neo4j error has no code and is not retryable
    assert not retry.is_retryable(ValueError("boom"))
    assert retry.error_code(ValueError("boom")) is None


# --- #19 retry loop --------------------------------------------------------------

def test_retry_succeeds_after_transient(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)  # no real backoff in tests
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("still settling")
        return "ok"

    assert retry.retry_bolt(flaky, attempts=5, base=0.0) == "ok"
    assert calls["n"] == 3


def test_retry_gives_up_and_reraises(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    calls = {"n": 0}

    def always():
        calls["n"] += 1
        raise ServiceUnavailable("down")

    with pytest.raises(ServiceUnavailable):
        retry.retry_bolt(always, attempts=3, base=0.0)
    assert calls["n"] == 3


def test_non_retryable_propagates_immediately(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    calls = {"n": 0}

    def bad():
        calls["n"] += 1
        raise _coded("Neo.ClientError.Statement.SyntaxError")

    with pytest.raises(ClientError):
        retry.retry_bolt(bad, attempts=5, base=0.0)
    assert calls["n"] == 1  # no retry


def test_auth_expired_invokes_rebuild(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    rebuilds = {"n": 0}
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _coded("Neo.ClientError.Security.TokenExpired")
        return "ok"

    out = retry.retry_bolt(flaky, attempts=3, base=0.0,
                           on_auth_expired=lambda: rebuilds.__setitem__("n", rebuilds["n"] + 1))
    assert out == "ok"
    assert rebuilds["n"] == 1  # driver rebuilt once on the auth-expired attempt
