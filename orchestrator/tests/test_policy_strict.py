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
