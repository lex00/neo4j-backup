"""#18 pluggable secret provider — no live cluster / no AWS needed."""

import pytest

from neo4j_backup_core import secrets
from neo4j_backup_core.clients import Neo4jClient


def test_env_provider_default_var(monkeypatch):
    monkeypatch.setenv("NEO4J_PASSWORD", "sekret")
    assert secrets.EnvSecretProvider().resolve() == "sekret"


def test_env_provider_custom_ref(monkeypatch):
    monkeypatch.setenv("MY_PW", "other")
    assert secrets.EnvSecretProvider().resolve("MY_PW") == "other"


def test_env_provider_missing_raises(monkeypatch):
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="NEO4J_PASSWORD"):
        secrets.EnvSecretProvider().resolve()


def test_build_and_from_env(monkeypatch):
    assert isinstance(secrets.build("env"), secrets.EnvSecretProvider)
    assert isinstance(secrets.build("aws-sm"), secrets.AwsSmSecretProvider)
    with pytest.raises(RuntimeError, match="unknown SECRET_PROVIDER"):
        secrets.build("bogus")
    monkeypatch.setenv("SECRET_PROVIDER", "aws-sm")
    assert isinstance(secrets.from_env(), secrets.AwsSmSecretProvider)


def test_aws_sm_needs_ref():
    with pytest.raises(RuntimeError, match="NEO4J_PASSWORD_REF"):
        secrets.AwsSmSecretProvider().resolve(None)


def test_client_reuses_its_driver_and_reresolves_after_a_failure(monkeypatch):
    """#230: one driver per client; rotation: a failure drops it, so the next connect
    re-resolves the callable password."""
    import neo4j

    captured = []

    class _FakeDriver:
        def close(self):
            pass

    def _fake_driver(uri, auth=None):
        captured.append(auth[1])
        return _FakeDriver()

    monkeypatch.setattr(neo4j.GraphDatabase, "driver", staticmethod(_fake_driver))

    values = iter(["pw1", "pw2"])
    client = Neo4jClient("neo4j://x", "neo4j", lambda: next(values))
    with client._driver():
        pass
    with client._driver():
        pass
    assert captured == ["pw1"]  # reused, not re-resolved per statement
    with pytest.raises(RuntimeError):
        with client._driver():
            raise RuntimeError("auth expired")
    with client._driver():
        pass
    assert captured == ["pw1", "pw2"]  # the failure dropped it; the next connect re-resolved


def test_client_replaces_an_old_driver(monkeypatch):
    import neo4j
    captured = []

    class _FakeDriver:
        def close(self):
            pass
    monkeypatch.setattr(neo4j.GraphDatabase, "driver",
                        staticmethod(lambda uri, auth=None: (captured.append(1), _FakeDriver())[1]))
    monkeypatch.setenv("NEO4J_DRIVER_MAX_AGE", "0")
    client = Neo4jClient("neo4j://x", "neo4j", "p")
    with client._driver():
        pass
    with client._driver():
        pass
    assert len(captured) == 2


def test_client_plain_string_password_still_works(monkeypatch):
    import neo4j

    captured = []

    class _FakeDriver:
        def close(self):
            pass

    monkeypatch.setattr(
        neo4j.GraphDatabase, "driver",
        staticmethod(lambda uri, auth=None: (captured.append(auth[1]), _FakeDriver())[1]),
    )
    with Neo4jClient("neo4j://x", "neo4j", "static")._driver():
        pass
    assert captured == ["static"]
