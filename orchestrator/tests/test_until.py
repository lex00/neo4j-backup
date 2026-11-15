"""#77(a): point-in-time values are parsed at every boundary; only a normalised datetime string
reaches Cypher."""

import pytest

from neo4j_backup_core.clients import Neo4jClient
from neo4j_backup_core.until import UntilError, parse_until


@pytest.mark.parametrize("raw,norm", [
    ("2026-07-11T14:32:00", "2026-07-11T14:32:00+00:00"),       # AGENTS.md example: UTC
    ("2026-07-11T14:32:00Z", "2026-07-11T14:32:00+00:00"),
    ("2026-07-11T16:32:00+02:00", "2026-07-11T14:32:00+00:00"),  # converted to UTC
    ("2026-07-11T14:32:00.250Z", "2026-07-11T14:32:00.250000+00:00"),
    (None, None), ("", None),
])
def test_parse_until(raw, norm):
    assert parse_until(raw) == norm


@pytest.mark.parametrize("bad", [
    "2026-07-11') , seedURI: 's3://evil/x",        # injection-shaped
    "yesterday", "2026-07-11", "14:32", "2026-13-01T00:00:00", "2026-07-11T14:32:00+99:00",
])
def test_rejects(bad):
    with pytest.raises(UntilError):
        parse_until(bad)


class Recorder(Neo4jClient):
    def __init__(self):
        super().__init__()
        self.cypher = []

    def run_system(self, cypher, **params):
        self.cypher.append(cypher)
        return []


def test_seed_database_writes_the_normalised_value():
    neo = Recorder()
    neo.seed_database("db", "s3://b/k.backup", restore_until="2026-07-11T16:32:00+02:00")
    assert "seedRestoreUntil: datetime('2026-07-11T14:32:00+00:00')" in neo.cypher[0]
    with pytest.raises(UntilError):
        neo.seed_database("db", "s3://b/k.backup", restore_until="x') RETURN 1 //")
    assert len(neo.cypher) == 1


def test_cli_rejects_a_bad_until_as_usage():
    from neo4j_backup_cli.__main__ import build_parser
    with pytest.raises(SystemExit) as e:
        build_parser().parse_args(["restore", "demo", "--until", "2026-07-11') //"])
    assert e.value.code == 2
    args = build_parser().parse_args(["restore", "demo", "--until", "2026-07-11T14:32:00"])
    assert args.until == "2026-07-11T14:32:00+00:00"


def test_mcp_refuses_a_bad_until_before_any_call(monkeypatch):
    from neo4j_backup_mcp import tools
    monkeypatch.setattr(tools, "load_policy", lambda p: (_ for _ in ()).throw(AssertionError("called")))
    with pytest.raises(UntilError):
        tools.run_restore("demo", until="2026-07-11') //", dry_run=True)
