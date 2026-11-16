"""#123: freshness against rpo_minutes."""

import types
from datetime import datetime, timedelta, timezone

from neo4j_backup_core import freshness, paths
from neo4j_backup_core import policy as P

L = paths.DefaultPathLayout()
NOW = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)


class Store:
    def __init__(self, arts=None, loads=None):
        self.arts, self.loads = arts or {}, loads or {}

    def list_artifacts(self, prefix):
        return self.arts.get(prefix, [])

    def list_load_artifacts(self, prefix):
        return self.loads.get(prefix, [])


def group(gid, names, rpo=60, writes="read-write", protect=None):
    return types.SimpleNamespace(id=gid, names=names, rpo_minutes=rpo, tenant=None, cluster=None,
                                 load=types.SimpleNamespace(writes=writes, protect_writes=protect))


def art(gid, alias, minutes_ago):
    p = L.alias_prefix(gid, alias)
    return p, [(f"{p}x/a.backup", 1, NOW - timedelta(minutes=minutes_ago))]


def test_ok_stale_never_and_by_design():
    arts = dict([art("g", "fresh", 30), art("g", "old", 90)])
    s = Store(arts)
    out = freshness.status(s, L, [group("g", ["fresh", "old", "never"]),
                                  group("ro", ["cat"], writes="read-only"),
                                  group("scratch", ["tmp"], protect=False)], now=NOW)
    states = {r["target"]: r["state"] for r in out["targets"]}
    assert states == {"g/fresh": "ok", "g/old": "stale", "g/never": "stale",
                      "ro/cat": "by_design", "scratch/tmp": "by_design"}
    assert out["stale"] == ["g/old", "g/never"]


def test_a_load_is_a_restorable_point():
    lp = L.load_prefix("g", "m")
    s = Store(dict([art("g", "m", 500)]),
              {lp: [(f"{lp}t/m.dump", NOW - timedelta(minutes=10), "dump")]})
    row = freshness.status(s, L, [group("g", ["m"])], now=NOW)["targets"][0]
    assert row["state"] == "ok" and row["age_minutes"] == 10.0


RAW = {"profiles": {"slow": {"tier": "bronze", "rpo_minutes": 1440}},
       "db_groups": [{"id": "a", "tier": "bronze", "aliases": ["a"]},
                     {"id": "b", "profile": "slow", "aliases": ["b"]},
                     {"id": "c", "tier": "nightly", "aliases": ["c"],
                      "load": {"mode": "rebuild", "writes": "read-only",
                               "build": {"kind": "import", "input_type": "csv",
                                         "args": ["--nodes=x.csv"]}}}],
       "tiers": {"bronze": {"full_cron": "0 4 * * 0", "diff_cron": "0 0 * * *"},
                 "nightly": {"rebuild_cron": "0 1 * * *"}}}


def test_policy_warns_when_the_cadence_misses_rpo_and_profiles_set_it():
    p = P.Policy.model_validate(RAW)
    assert p.group("b").rpo_minutes == 1440  # from the profile (#101)
    rpo = [w for w in p.warnings() if "rpo_minutes" in w]
    assert any("'a'" in w and "diff_cron" in w for w in rpo)
    assert any("'c'" in w and "rebuild_cron" in w for w in rpo)  # load-only: its rebuilds
    assert not any("'b'" in w for w in rpo)
