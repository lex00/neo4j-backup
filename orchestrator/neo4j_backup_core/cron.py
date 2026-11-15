"""Just enough of 5-field cron to reason about a schedule's spacing (#73): when do fires happen,
and what is the longest gap between two of them. Fields: minute hour day-of-month month
day-of-week, each `*`, a number, a range `a-b`, a list `a,b`, and steps `*/n` or `a-b/n`.
Day-of-week 0 and 7 are Sunday. As in Vixie cron, when both day fields are restricted a day
matches if either does. No names (`MON`, `JAN`) or `@daily`-style macros."""

from __future__ import annotations

from datetime import date, datetime, timedelta

_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]


def _field(text: str, lo: int, hi: int) -> set[int]:
    out: set[int] = set()
    for part in text.split(","):
        base, _, step = part.partition("/")
        if base == "*":
            a, b = lo, hi
        elif "-" in base:
            a, b = (int(x) for x in base.split("-", 1))
        else:
            a = b = int(base)
            if step:
                b = hi
        n = int(step) if step else 1
        if not (lo <= a <= hi and lo <= b <= hi) or n < 1 or a > b:
            raise ValueError(f"cron field {text!r} out of range {lo}-{hi}")
        out.update(range(a, b + 1, n))
    return out


def parse(expr: str):
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron {expr!r}: expected 5 fields")
    sets = [_field(p, lo, hi) for p, (lo, hi) in zip(parts, _RANGES)]
    dows = {0 if d == 7 else d for d in sets[4]}
    return sets[0], sets[1], sets[2], sets[3], dows, parts[2] != "*", parts[4] != "*"


def fires(expr: str, start: date, days: int):
    """Every fire time from `start` for `days` days, in order."""
    minutes, hours, doms, months, dows, dom_set, dow_set = parse(expr)
    for i in range(days):
        d = start + timedelta(days=i)
        if d.month not in months:
            continue
        dom_ok, dow_ok = d.day in doms, (d.isoweekday() % 7) in dows
        if dom_set and dow_set:
            ok = dom_ok or dow_ok
        else:
            ok = dom_ok and dow_ok
        if not ok:
            continue
        for h in sorted(hours):
            for m in sorted(minutes):
                yield datetime(d.year, d.month, d.day, h, m)


def max_gap(expr: str, start: date = date(2027, 1, 1), days: int = 800) -> timedelta | None:
    """The longest time between two consecutive fires over `days` days (two years by default,
    so month lengths and a leap day are covered). None if it fires fewer than twice."""
    prev = None
    gap = None
    for t in fires(expr, start, days):
        if prev is not None and (gap is None or t - prev > gap):
            gap = t - prev
        prev = t
    return gap
