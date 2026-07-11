"""#58 P1 — the `neo4j-backup` CLI honours the #60 contract on every subcommand: conformant
`--json` envelope, documented exit codes, and dry-run / --confirm / blast-radius on the guarded
(mutating) commands. `ops.*` is stubbed — this pins the CLI's contract behaviour, not the ops
(those are `test_ops.py`)."""

import json
import types

import pytest

from neo4j_backup_cli import __main__ as cli
from neo4j_backup_core.cli_contract import Exit

from cli_contract import assert_conformant, assert_exit


class _Group:
    id = "demo"
    names = ["orders", "customers"]
    restore_mode = "alias-swap"


@pytest.fixture(autouse=True)
def stub_env(monkeypatch):
    """No live Neo4j / S3 / policy: env builders and load_policy return inert stand-ins."""
    monkeypatch.setattr(cli.env, "store", lambda: object())
    monkeypatch.setattr(cli.env, "neo4j", lambda: object())
    monkeypatch.setattr(cli.env, "runner", lambda: types.SimpleNamespace(env=lambda: {}))
    monkeypatch.setattr(cli.env, "policy_path", lambda: "x.yaml")
    pol = types.SimpleNamespace(
        group=lambda gid: _Group(),
        partition_keys=lambda: ["demo/orders", "demo/customers"],
    )
    monkeypatch.setattr(cli, "load_policy", lambda p: pol)
    return monkeypatch


def run(argv):
    """Parse argv and invoke the handler — exercises the parser + command, returns (envelope, code)."""
    args = cli.build_parser().parse_args(argv)
    return args.func(args)


# --- unguarded commands -----------------------------------------------------------------------

def test_targets_conformant():
    env, code = run(["targets"])
    assert_conformant(env, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert env["result"]["targets"] == ["demo/orders", "demo/customers"]


def test_import_passes_through_and_is_conformant(monkeypatch):
    seen = {}
    monkeypatch.setattr(cli.env, "runner", lambda: object())
    monkeypatch.setattr(cli.ops, "import_database",
                        lambda run, runner, db, src: seen.update(db=db, src=src) or {"database": db, "argv": []})
    env, code = run(["import", "orders-x", "--nodes=/n.csv", "--relationships=/r.csv"])
    assert_conformant(env, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert seen == {"db": "orders-x", "src": ["--nodes=/n.csv", "--relationships=/r.csv"]}


def test_backup_runs_each_member(monkeypatch):
    seen = []
    monkeypatch.setattr(cli.ops, "backup_target",
                        lambda *a, **k: seen.append(a[6]) or {"artifact": f"{a[6]}.backup"})
    env, code = run(["backup", "demo", "--kind", "FULL"])
    assert_conformant(env, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert seen == ["orders", "customers"]  # one backup per group member
    assert len(env["result"]["backups"]) == 2


# --- guarded commands: restore ----------------------------------------------------------------

def _no_mutate(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("apply path ran during a dry-run / guard refusal")
    monkeypatch.setattr(cli.ops, "restore_group", boom)


def test_restore_dry_run_previews_without_mutating(monkeypatch):
    _no_mutate(monkeypatch)
    monkeypatch.setattr(cli.ops, "plan_restore",
                        lambda *a, **k: {"mode": "by-name", "members": [], "drops": ["orders"]})
    env, code = run(["restore", "demo", "--dry-run"])
    assert_conformant(env, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert env["result"]["dry_run"] is True
    assert env["result"]["plan"]["drops"] == ["orders"]  # blast radius surfaced


def test_restore_without_confirm_is_guard_refusal(monkeypatch):
    _no_mutate(monkeypatch)
    monkeypatch.setattr(cli.ops, "plan_restore",
                        lambda *a, **k: {"mode": "alias-swap", "members": [], "drops": []})
    env, code = run(["restore", "demo"])
    assert_conformant(env, expect_ok=False)
    assert_exit(code, Exit.GUARD)
    assert env["error"]["kind"] == "confirm_required"


def test_restore_with_confirm_applies(monkeypatch):
    monkeypatch.setattr(cli.ops, "plan_restore", lambda *a, **k: {"drops": []})
    applied = {}
    monkeypatch.setattr(cli.ops, "restore_group",
                        lambda *a, **k: applied.setdefault("ran", True) or {"members": []})
    env, code = run(["restore", "demo", "--confirm"])
    assert_conformant(env, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert applied["ran"] is True


# --- guarded commands: prune ------------------------------------------------------------------

def test_prune_dry_run_and_guard(monkeypatch):
    monkeypatch.setattr(cli.ops, "prune",
                        lambda *a, dry_run=False, **k: {"deleted": 3, "detail": {}, "keys": ["a", "b", "c"],
                                                        "dry_run": dry_run})
    dry, code = run(["prune", "--dry-run"])
    assert_conformant(dry, expect_ok=True)
    assert_exit(code, Exit.OK)
    assert dry["result"]["deleted"] == 3 and dry["result"]["keys"] == ["a", "b", "c"]

    refused, code2 = run(["prune"])
    assert_conformant(refused, expect_ok=False)
    assert_exit(code2, Exit.GUARD)


# --- main(): stdout JSON, exit codes ----------------------------------------------------------

def test_main_json_emits_one_conformant_object(capsys):
    code = cli.main(["--json", "targets"])
    assert code == int(Exit.OK)
    out = json.loads(capsys.readouterr().out)  # exactly one JSON object on stdout
    from neo4j_backup_core.cli_contract import validate_envelope
    assert validate_envelope(out) == []


def test_main_bad_args_exit_usage():
    with pytest.raises(SystemExit) as e:
        cli.main(["definitely-not-a-command"])
    assert e.value.code == int(Exit.USAGE)  # argparse -> exit 2


def test_main_op_error_maps_to_failure(monkeypatch, capsys):
    # metadata restore with no artifact raises OpError -> FAILURE envelope on stdout
    monkeypatch.setattr(cli.env, "store", lambda: types.SimpleNamespace(latest_text_key=lambda p: None))
    code = cli.main(["--json", "metadata", "restore"])
    assert code == int(Exit.FAILURE)
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False and out["error"]["kind"] == "op_failed"
