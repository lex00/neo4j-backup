"""#93: drop superseded physicals, never a customer's databases."""

from datetime import datetime, timedelta, timezone

import pytest

from neo4j_backup_core import naming, ops

NOW = datetime(2026, 11, 16, tzinfo=timezone.utc)


class G:
    id, restore_mode, names = "g", "alias-swap", ["orders"]


def phys(days_ago):
    t = (NOW - timedelta(days=days_ago)).strftime("%Y%m%dt%H%M%S")
    return naming.physical("orders", t)


class Neo:
    def __init__(self):
        self.dbs = {phys(d): NOW - timedelta(days=d) for d in (1, 5, 9, 20)}
        self.dbs["orders-made-by-hand"] = NOW - timedelta(days=50)
        self.dbs["customer-db"] = NOW - timedelta(days=50)
        self.current, self.dropped = phys(1), []

    def list_databases(self):
        return list(self.dbs) + [phys(1)]  # a duplicate row, as on a cluster

    def list_aliases(self):
        return {"orders": self.current, "report": phys(20)}  # another alias targets the oldest

    def alias_target(self, a):
        return self.current

    def database_created(self, d):
        return self.dbs.get(d)

    def drop_database(self, d):
        self.dropped.append(d)

    def privilege_commands(self, d):
        return [f"GRANT ACCESS ON DATABASE `{d}` TO `app`"]

    def run_system(self, q, **p):
        self.dropped.append(q)


def test_plan_keeps_current_and_previous_and_never_customer_databases():
    plan = ops.plan_cleanup(Neo(), G(), keep=1, now=NOW)
    m = plan["members"][0]
    assert m["current"] == phys(1) and m["kept"] == [phys(5)]
    assert plan["drops"] == [phys(9)]  # phys(20) is targeted by another alias
    assert "customer-db" not in plan["drops"] and "orders-made-by-hand" not in plan["drops"]


def test_older_than():
    assert ops.plan_cleanup(Neo(), G(), keep=1, older_than_days=7, now=NOW)["drops"] == [phys(9)]
    assert ops.plan_cleanup(Neo(), G(), keep=1, older_than_days=10, now=NOW)["drops"] == []
    with pytest.raises(ops.OpError):
        ops.plan_cleanup(Neo(), G(), keep=0, now=NOW)  # keep counts are >= 1 (#78)


def test_cleanup_drops_exactly_the_plan():
    neo = Neo()
    out = ops.cleanup(neo, G(), keep=1)
    assert out["drops"] == [phys(9)]
    assert neo.dropped == [f"REVOKE GRANT ACCESS ON DATABASE `{phys(9)}` FROM `app`", phys(9)]


def test_revoke():
    from neo4j_backup_core.privileges import revoke
    assert revoke("DENY WRITE ON GRAPH `x` TO `r`") == "REVOKE DENY WRITE ON GRAPH `x` FROM `r`"


def test_tier_cleanup_settings_default_to_a_week_of_soak():
    """#231: the scheduled cleanup keeps one previous database and anything under 7 days old."""
    from neo4j_backup_core import policy as P
    p = P.Policy.model_validate({
        "db_groups": [{"id": "g", "tier": "gold", "aliases": ["orders"]}],
        "tiers": {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *",
                           "cleanup_cron": "0 3 * * *"},
                  "fast": {"full_cron": "0 2 * * *", "cleanup_keep": 2,
                           "cleanup_older_than_days": 1}}})
    assert (p.tiers["gold"].cleanup_keep, p.tiers["gold"].cleanup_older_than_days) == (1, 7)
    assert (p.tiers["fast"].cleanup_keep, p.tiers["fast"].cleanup_older_than_days) == (2, 1)
