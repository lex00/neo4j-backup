"""#75: a policy that fails to load must fail loudly where schedules and DAGs are built, not
leave an orchestrator with no schedules."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from neo4j_backup_core import policy as P

REPO = Path(__file__).resolve().parents[2]
GOOD = REPO / "policies" / "demo.yaml"


@pytest.fixture(autouse=True)
def _clear_cache():
    P._cache.clear()
    yield
    P._cache.clear()


def test_no_policy_configured_returns_none(tmp_path):
    assert P.load_policy_at_startup(tmp_path / "absent.yaml", configured=False) is None


def test_configured_but_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        P.load_policy_at_startup(tmp_path / "absent.yaml", configured=True)


def test_malformed_policy_raises_without_retrying(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("db_groups: [ {id: x, tier: nope} ]\ntiers: {}\n")
    waits = []
    with pytest.raises(ValueError):
        P.load_policy_at_startup(bad, configured=True, sleep=waits.append)
    assert waits == []


calls = {"n": 0}


def flaky_loader(source):  # POLICY_LOADER target: fails twice, then serves the demo policy
    calls["n"] += 1
    if calls["n"] < 3:
        raise ConnectionError("s3 blip")
    return GOOD.read_text()


def test_transient_fetch_errors_are_retried_with_backoff(monkeypatch):
    calls["n"] = 0
    monkeypatch.setenv("POLICY_LOADER", f"{__name__}.flaky_loader")
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    waits = []
    pol = P.load_policy_at_startup("s3://bucket/policy.yaml", configured=True, delay=1.0,
                                   sleep=waits.append)
    assert pol.db_groups and waits == [1.0, 2.0]


def test_transient_errors_give_up_after_the_attempts(monkeypatch):
    calls["n"] = -100  # never succeeds within 3 attempts
    monkeypatch.setenv("POLICY_LOADER", f"{__name__}.flaky_loader")
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    waits = []
    with pytest.raises(ConnectionError):
        P.load_policy_at_startup("s3://b/p.yaml", configured=True, attempts=3, sleep=waits.append)
    assert waits == [1.0, 2.0]


def _dagster(policy: Path):
    pytest.importorskip("dagster")
    env = {**os.environ, "NEO4J_BACKUP_POLICY": str(policy), "NEO4J_PASSWORD": "dummy"}
    validate = subprocess.run(
        [str(Path(sys.executable).parent / "dagster"), "definitions", "validate",
         "-m", "neo4j_backup_dagster.definitions"], env=env, capture_output=True, text=True)
    names = subprocess.run(
        [sys.executable, "-c", "from neo4j_backup_dagster.definitions import defs; "
         "print(sorted(s.name for s in defs.schedules))"], env=env, capture_output=True, text=True)
    return validate, names


def test_dagster_code_location_fails_on_a_bad_policy(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("db_groups: [ {id: x, aliases: [a], tier: nope, s3_prefix: 's3://x/'} ]\n"
                   "tiers: {}\n")
    validate, _ = _dagster(bad)
    assert validate.returncode != 0
    assert "unknown tier 'nope'" in validate.stdout + validate.stderr


def test_dagster_lists_schedules_for_a_good_policy():
    validate, names = _dagster(GOOD)
    assert validate.returncode == 0, validate.stdout + validate.stderr
    assert "gold_diff" in names.stdout and "gold_full" in names.stdout
