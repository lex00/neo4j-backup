"""#73: the cron reader behind the retention warning."""

from datetime import date, timedelta

import pytest

from neo4j_backup_core import cron


@pytest.mark.parametrize("expr,gap", [
    ("0 2 * * *", timedelta(days=1)),          # gold: daily
    ("0 3 * * 0", timedelta(days=7)),          # silver: weekly
    ("0 * * * *", timedelta(hours=1)),
    ("0 */6 * * *", timedelta(hours=6)),
    ("0 1 1 * *", timedelta(days=31)),         # monthly: the longest month
    ("0 0 * * 1-5", timedelta(days=3)),        # weekdays: Friday to Monday
    ("30 4 1,15 * *", timedelta(days=17)),     # 15th to the 1st after a 31-day month
    ("0 0 1 * 0", timedelta(days=7)),          # both day fields set: either matches
    ("0 0 * * 7", timedelta(days=7)),          # 7 is Sunday
])
def test_max_gap(expr, gap):
    assert cron.max_gap(expr) == gap


def test_fires_in_order():
    got = list(cron.fires("15 9,21 * * *", date(2027, 1, 1), 1))
    assert [t.strftime("%H:%M") for t in got] == ["09:15", "21:15"]


@pytest.mark.parametrize("bad", ["* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "5-1 * * * *"])
def test_rejects_bad_expressions(bad):
    with pytest.raises(ValueError):
        cron.parse(bad)
