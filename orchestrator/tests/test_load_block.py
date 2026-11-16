"""#103: the `load:` block (schema and validation; behaviour is #107, #108, #112)."""

import pytest

from neo4j_backup_core.policy import Policy, load_policy

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"},
         "nightly": {"rebuild_cron": "0 1 * * *"},
         "both": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *", "rebuild_cron": "0 1 * * *"}}
BUILD = {"kind": "import", "input_type": "parquet", "args": ["--nodes=P=p/*.parquet"]}


def policy(load=None, tier="gold", **group):
    g = {"id": "cust", "aliases": ["a"], "tier": tier, "s3_prefix": "s3://x/", **group}
    if load is not None:
        g["load"] = load
    return Policy.model_validate({"db_groups": [g], "tiers": TIERS})


def test_no_load_block_is_live_as_before():
    load = policy().db_groups[0].load
    assert (load.mode, load.writes, load.protect_writes, load.keep_loads, load.baseline) == \
        ("live", "read-write", True, 14, True)
    assert load.source is None and load.build is None


def test_demo_policy_unchanged():
    p = load_policy("policies/demo.yaml")
    assert all(g.load.mode == "live" for g in p.db_groups)


@pytest.mark.parametrize("load,tier", [
    ({"mode": "rebuild", "writes": "read-only", "build": BUILD}, "nightly"),
    ({"mode": "rebuild", "writes": "read-write", "protect_writes": False, "build": BUILD}, "nightly"),
    ({"mode": "rebuild", "writes": "read-write", "build": BUILD}, "both"),
    ({"mode": "once", "writes": "read-write"}, "gold"),
    ({"mode": "once", "writes": "read-only"}, "gold"),
])
def test_the_six_customer_paths_load(load, tier):
    policy(load, tier=tier)


def test_protect_writes_defaults():
    assert policy({"mode": "once"}).db_groups[0].load.protect_writes is True
    ro = policy({"mode": "once", "writes": "read-only"}).db_groups[0].load
    assert ro.protect_writes is None and not ro.backs_up_writes


def test_source_and_build_fields():
    load = policy({"mode": "rebuild", "writes": "read-only", "build": BUILD,
                   "source": {"storage": "default", "prefix": "catalog/_source/"}},
                  tier="nightly").db_groups[0].load
    assert load.source.prefix == "catalog/_source/" and load.build.args == BUILD["args"]


@pytest.mark.parametrize("load,tier,msg", [
    ({"mode": "live", "writes": "read-only"}, "gold", "can't be read-only"),
    ({"mode": "live", "protect_writes": False}, "gold", "always backs up"),
    ({"mode": "once", "writes": "read-only", "protect_writes": True}, "gold", "needs writes: read-write"),
    ({"mode": "rebuild", "writes": "read-only", "build": BUILD}, "gold", "rebuild_cron"),
    ({"mode": "rebuild", "writes": "read-only"}, "nightly", "needs a build"),
    ({"keep_loads": 0}, "gold", "keep_loads"),
    ({"mode": "rebuild", "writes": "read-only", "build": {"kind": "import"}}, "nightly", "input_type"),
    ({"mode": "rebuild", "writes": "read-only", "build": {"kind": "command"}}, "nightly", "command or an image"),
    ({"mode": "rebuild", "writes": "read-write", "build": BUILD}, "nightly", "full_cron and diff_cron"),
    ({"mdoe": "once"}, "gold", "mdoe"),
])
def test_rejected_combinations_name_the_group(load, tier, msg):
    with pytest.raises(ValueError) as e:
        policy(load, tier=tier)
    assert "cust" in str(e.value) and msg in str(e.value)


def test_a_tier_needs_some_cron():
    with pytest.raises(ValueError, match="at least one"):
        Policy.model_validate({"db_groups": [], "tiers": {"empty": {}}})


def test_policy_md_example_loads():
    import re
    from pathlib import Path

    import yaml
    text = (Path(__file__).resolve().parents[2] / "POLICY.md").read_text()
    example = re.search(r"```yaml\n(.*?)```", text, re.S).group(1)
    p = Policy.model_validate(yaml.safe_load(example))
    assert {g.id: g.load.mode for g in p.db_groups}["catalog"] == "rebuild"
