"""#140: a failed seed leaves no orphan physicals, and the CLI keeps its envelope."""

import json

import pytest

from neo4j_backup_cli import __main__ as cli
from neo4j_backup_core import ops, paths
from test_artifacts import MemStore
from test_choose_seed import Neo

L = paths.DefaultPathLayout()


class G:
    id, restore_mode, names = "g", "alias-swap", ["orders", "graph"]

    def topology_for(self, n):
        return None


class Failing(Neo):
    def __init__(self):
        super().__init__()
        self.dropped = []

    def seed_database(self, name, uri, restore_until=None, **k):
        if name.startswith("graph"):
            raise RuntimeError("seedRestoreUntil cannot be used with this type of seed")
        super().seed_database(name, uri, restore_until, **k)

    def drop_database(self, n):
        self.dropped.append(n)


def store():
    s = MemStore()
    for n in G.names:
        s.add(f"g/{n}/{n}-1/full.backup")
    return s


def test_second_member_fails_first_is_dropped():
    neo = Failing()
    with pytest.raises(ops.SeedError) as e:
        ops.restore_group(neo, store(), G(), L)
    first = neo.seeded[0][0]
    assert e.value.dropped == [first] and neo.dropped == [first] and neo.altered == []
    assert first in str(e.value)


def test_cli_wraps_any_exception_in_the_envelope(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_policy", lambda p: (_ for _ in ()).throw(
        type("ClientError", (Exception,), {"__module__": "neo4j.exceptions"})("nope")))
    monkeypatch.delenv("NEO4J_BACKUP_DEBUG", raising=False)
    code = cli.main(["--json", "targets"])
    out = capsys.readouterr()
    env = json.loads(out.out)
    assert code == 1 and env["ok"] is False and env["error"]["kind"] == "neo4j_error"
    assert "Traceback" not in out.err
    monkeypatch.setenv("NEO4J_BACKUP_DEBUG", "1")
    cli.main(["--json", "targets"])
    assert "Traceback" in capsys.readouterr().err


def test_cli_seed_failure_lists_the_dropped(monkeypatch, capsys):
    def boom(*a, **k):
        raise ops.SeedError("g/graph: seeding failed", ["orders-x"], [])
    parser = cli.build_parser()
    parser._subparsers._group_actions[0].choices["targets"].set_defaults(func=boom)
    monkeypatch.setattr(cli, "build_parser", lambda: parser)
    code = cli.main(["--json", "targets"])
    env = json.loads(capsys.readouterr().out)
    assert code == 1 and env["error"]["kind"] == "seed_failed"
    assert env["result"]["dropped"] == ["orders-x"]
