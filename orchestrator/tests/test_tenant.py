"""#102: tenant is a label for selecting a customer's groups (not an access boundary)."""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from neo4j_backup_core.policy import Policy, SelectionError

REPO = Path(__file__).resolve().parents[2]
TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def raw(**extra):
    return {"profiles": {"acme-app": {"tier": "gold", "tenant": "acme"}},
            "db_groups": [
                {"id": "orders", "aliases": ["orders"], "tier": "gold", "s3_prefix": "s3://x/",
                 "tenant": "acme"},
                {"id": "audit", "profile": "acme-app", "aliases": ["audit"], "s3_prefix": "s3://x/"},
                {"id": "billing", "aliases": ["billing"], "tier": "gold", "s3_prefix": "s3://x/",
                 "owner": "globex", **extra}],
            "tiers": TIERS}


def test_select_by_tenant_including_via_profile():
    p = Policy.model_validate(raw())
    assert [g.id for g in p.select(tenant="acme")] == ["orders", "audit"]
    assert [g.id for g in p.select("billing")] == ["billing"]
    assert p.tenants() == ["acme", "globex"]


def test_owner_maps_to_tenant_with_a_warning(caplog):
    with caplog.at_level(logging.WARNING):
        p = Policy.model_validate(raw())
    assert p.group("billing").tenant == "globex" and "'owner'" in caplog.text


@pytest.mark.parametrize("group,tenant,msg", [
    ("orders", "acme", "not both"), (None, None, "give a group"),
    (None, "initech", "tenants: acme, globex"), ("nope", None, "unknown group"),
])
def test_bad_selection(group, tenant, msg):
    with pytest.raises(SelectionError, match=msg):
        Policy.model_validate(raw()).select(group, tenant)


def _cli(policy, *args):
    env = {**os.environ, "NEO4J_BACKUP_POLICY": str(policy)}
    p = subprocess.run([sys.executable, "-m", "neo4j_backup_cli", "--json", *args],
                       capture_output=True, text=True, env=env)
    return p.returncode, json.loads(p.stdout)


@pytest.fixture
def pol(tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw()))
    return f


def test_cli_targets_filters_and_shows_tenants(pol):
    code, out = _cli(pol, "targets", "--tenant", "acme")
    assert code == 0 and out["result"]["targets"] == ["orders/orders", "audit/audit"]
    assert out["result"]["tenants"] == {"orders": "acme", "audit": "acme"}
    code, out = _cli(pol, "targets")
    assert out["result"]["tenants"]["billing"] == "globex"


@pytest.mark.parametrize("args", [["backup", "orders", "--tenant", "acme"],
                                  ["verify", "--tenant", "initech"],
                                  ["aggregate", "--dry-run"]])
def test_cli_selection_errors_are_usage(pol, args):
    code, out = _cli(pol, *args)
    assert code == 2 and out["error"]["kind"] == "usage"


def test_restore_has_no_tenant_option(pol):
    p = subprocess.run([sys.executable, "-m", "neo4j_backup_cli", "restore", "--tenant", "acme"],
                       capture_output=True, text=True,
                       env={**os.environ, "NEO4J_BACKUP_POLICY": str(pol)})
    assert p.returncode == 2 and "--tenant" in p.stderr


def test_cli_by_tenant_runs_each_group(monkeypatch):
    import types
    from neo4j_backup_cli import __main__ as cli
    p = Policy.model_validate(raw())
    monkeypatch.setattr(cli, "load_policy", lambda _p: p)
    monkeypatch.setattr(cli.env, "policy_path", lambda: "x")
    seen = []

    def fake(args):
        seen.append(args.group)
        return cli.envelope("verify", ok=True, group=args.group, result={"verified": []}), \
            cli.Exit.OK
    run = cli._by_tenant(fake, "verify")
    env, code = run(types.SimpleNamespace(group=None, tenant="acme"))
    assert seen == ["orders", "audit"] and env["result"]["tenant"] == "acme"
    assert [g["group"] for g in env["result"]["groups"]] == ["orders", "audit"]


def test_mcp_tenant(monkeypatch):
    from neo4j_backup_mcp import tools
    p = Policy.model_validate(raw())
    monkeypatch.setattr(tools, "load_policy", lambda _p: p)
    monkeypatch.setattr(tools.env, "store", lambda: type("S", (), {"list_artifacts": lambda s, x: []})())
    assert tools.list_targets(tenant="acme")["targets"] == ["orders/orders", "audit/audit"]
    assert {r["tenant"] for r in tools.backup_status(tenant="globex")["targets"]} == {"globex"}
    with pytest.raises(SelectionError):
        tools.run_verify(group="orders", tenant="acme")
