"""#222: a misspelled policy key fails instead of silently taking the default."""

import yaml

from neo4j_backup_core import policy as P

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def check(tmp_path, raw):
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    return P.validate_file(f)


def test_misspelled_group_key_names_the_group(tmp_path):
    errs = check(tmp_path, {"db_groups": [{"id": "shop", "tier": "gold", "aliases": ["shop"],
                                           "retension_days": 30}], "tiers": TIERS})
    assert errs == ["group 'shop': retension_days: Extra inputs are not permitted"]


def test_misspelled_tier_and_top_level_keys(tmp_path):
    errs = check(tmp_path, {"db_groups": [{"id": "shop", "tier": "gold", "aliases": ["shop"]}],
                            "tiers": {"gold": {**TIERS["gold"], "ful_cron": "x"}},
                            "schedule": "running"})
    assert any("tiers.gold.ful_cron" in e for e in errs) and any(e.startswith("schedule") for e in errs)


def test_misspelled_key_in_a_profile(tmp_path):
    errs = check(tmp_path, {"profiles": {"p": {"tier": "gold", "rpo_minuts": 5}},
                            "db_groups": [{"id": "shop", "profile": "p", "aliases": ["shop"]}],
                            "tiers": TIERS})
    assert errs and "rpo_minuts" in errs[0]


def test_tier_rpo_is_the_default_for_its_groups(tmp_path):
    """#253: a tier's rpo_minutes applies to groups that don't set their own."""
    p = P.Policy.model_validate({
        "profiles": {"p": {"tier": "slow", "rpo_minutes": 30}},
        "db_groups": [{"id": "a", "tier": "slow", "aliases": ["aaa"]},
                      {"id": "b", "tier": "slow", "aliases": ["bbb"], "rpo_minutes": 5000},
                      {"id": "c", "profile": "p", "aliases": ["ccc"]}],
        "tiers": {"slow": {"full_cron": "0 4 * * 0", "diff_cron": "0 0 * * *",
                           "rpo_minutes": 1500}}})
    assert [p.group(g).rpo_minutes for g in "abc"] == [1500, 5000, 30]


def test_shipped_policies_validate_without_warnings():
    """#253: the files people copy don't page falsely."""
    from pathlib import Path
    repo = Path(__file__).resolve().parents[2]
    for f in ("policies/demo.yaml", "policies/example.yaml"):
        assert P.validate_file(repo / f) == [], f
        assert P.load_policy(repo / f).warnings() == [], f


def test_policy_md_complete_example_loads_without_warnings(tmp_path):
    """#253: the example people copy from POLICY.md doesn't page falsely."""
    import re
    from pathlib import Path
    text = (Path(__file__).resolve().parents[2] / "POLICY.md").read_text()
    block = text.split("## A complete policy", 1)[1]
    yml = re.search(r"```yaml\n(.*?)```", block, re.S).group(1)
    f = tmp_path / "p.yaml"
    f.write_text(yml)
    pol = P.load_policy(f)
    assert pol.warnings() == [], pol.warnings()
