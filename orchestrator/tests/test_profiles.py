"""#101: profiles name a customer type once; groups point at one and override what differs."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from neo4j_backup_core import policy as P

REPO = Path(__file__).resolve().parents[2]
BUILD = {"kind": "import", "input_type": "parquet", "args": ["--nodes=P=p/*.parquet"]}

RAW = {
    "profiles": yaml.safe_load((REPO / "policies" / "demo.yaml").read_text())["profiles"],
    "db_groups": [
        {"id": "shop", "profile": "app-live", "aliases": ["shop"], "s3_prefix": "s3://x/"},
        {"id": "recs-sandbox", "profile": "data-scratch", "aliases": ["recs"],
         "s3_prefix": "s3://x/", "load": {"keep_loads": 3, "build": BUILD}},
        {"id": "catalog", "profile": "data-interval", "aliases": ["catalog"],
         "s3_prefix": "s3://x/", "retention_days": 30, "load": {"build": BUILD}},
    ],
    "tiers": {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"},
              "nightly": {"rebuild_cron": "0 1 * * *"}},
}


def test_group_gets_profile_values_and_overrides_win():
    p = P.Policy.model_validate(RAW)
    shop, recs, cat = (p.group(g) for g in ("shop", "recs-sandbox", "catalog"))
    assert (shop.tier, shop.retention_days, shop.profile) == ("gold", 14, "app-live")
    assert (recs.tier, recs.load.mode, recs.load.protect_writes, recs.load.keep_loads) == \
        ("nightly", "rebuild", False, 3)  # load merged per key; keep_loads overridden
    assert cat.retention_days == 30 and cat.load.writes == "read-only"


def test_lists_are_replaced_not_merged():
    raw = json.loads(json.dumps(RAW))
    raw["profiles"]["data-interval"]["load"]["build"] = {**BUILD, "args": ["--from-profile"]}
    raw["db_groups"][2]["load"] = {"build": {**BUILD, "args": ["--from-group"]}}
    assert P.Policy.model_validate(raw).group("catalog").load.build.args == ["--from-group"]


def test_unknown_profile_names_the_group():
    raw = json.loads(json.dumps(RAW))
    raw["db_groups"][0]["profile"] = "app-livee"
    with pytest.raises(ValueError, match="'shop' uses unknown profile 'app-livee'"):
        P.Policy.model_validate(raw)


@pytest.mark.parametrize("key", ["id", "aliases", "databases", "overrides"])
def test_profiles_cannot_set_per_group_fields(key):
    raw = json.loads(json.dumps(RAW))
    raw["profiles"]["app-live"][key] = ["x"]
    with pytest.raises(ValueError, match="can't set"):
        P.Policy.model_validate(raw)


def test_unknown_profile_field_is_rejected():
    raw = json.loads(json.dumps(RAW))
    raw["profiles"]["app-live"]["tierr"] = "gold"
    with pytest.raises(ValueError, match="unknown group field"):
        P.Policy.model_validate(raw)


def test_existing_policies_load_unchanged():
    raw = yaml.safe_load((REPO / "policies" / "demo.yaml").read_text())
    raw.pop("profiles")
    with_p = P.load_policy(REPO / "policies" / "demo.yaml")
    without = P.Policy.model_validate(raw)
    assert [g.model_dump() for g in with_p.db_groups] == [g.model_dump() for g in without.db_groups]


def test_explain_says_where_each_value_came_from(tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(RAW))
    out = P.explain(f, "recs-sandbox")
    src = out["from"]
    assert src["tier"] == "profile:data-scratch" and src["aliases"] == "group"
    assert src["load.mode"] == "profile:data-scratch" and src["load.keep_loads"] == "group"
    assert src["rpo_minutes"] == "default" and out["group"]["load"]["keep_loads"] == 3


def _cli(*args, policy=None):
    env = {**os.environ, "NEO4J_BACKUP_POLICY": str(policy or REPO / "policies" / "demo.yaml")}
    return subprocess.run([sys.executable, "-m", "neo4j_backup_cli", "--json", *args],
                          capture_output=True, text=True, env=env)


def test_cli_policy_show(tmp_path):
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(RAW))
    proc = _cli("policy", "show", "shop", policy=f)
    out = json.loads(proc.stdout)
    assert proc.returncode == 0 and out["result"]["from"]["tier"] == "profile:app-live"


def test_cli_policy_validate(tmp_path):
    good = _cli("policy", "validate", str(REPO / "policies" / "demo.yaml"))
    assert good.returncode == 0, good.stdout + good.stderr
    bad = tmp_path / "bad.yaml"
    raw = json.loads(json.dumps(RAW))
    raw["db_groups"][0]["profile"] = "nope"
    raw["db_groups"][1]["load"]["keep_loads"] = 0
    bad.write_text(yaml.safe_dump(raw))
    proc = _cli("policy", "validate", str(bad))
    out = json.loads(proc.stdout)
    assert proc.returncode == 2 and out["error"]["kind"] == "invalid_policy"
    assert any("unknown profile 'nope'" in e for e in out["result"]["errors"])


# --- every front-end sees resolved groups ------------------------------------------------------

@pytest.fixture
def profiled(tmp_path, monkeypatch):
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(RAW))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    P._cache.clear()
    return f


def test_cli_targets_see_resolved_groups(profiled):
    proc = _cli("targets", policy=profiled)
    assert "shop/shop" in json.loads(proc.stdout)["result"]["targets"]


def test_mcp_sees_resolved_groups(profiled):
    from neo4j_backup_mcp import tools
    keys = tools.list_targets()
    assert "recs-sandbox/recs" in json.dumps(keys)


def test_dagster_sees_resolved_groups(profiled):
    pytest.importorskip("dagster")
    proc = subprocess.run(
        [sys.executable, "-c", "from neo4j_backup_dagster.definitions import defs; "
         "print(sorted(s.name for s in defs.schedules))"],
        env={**os.environ, "NEO4J_PASSWORD": "x"}, capture_output=True, text=True)
    assert "gold_full" in proc.stdout and "nightly_full" not in proc.stdout, proc.stderr
