"""Checks a new database must pass before customers see it (#111): run after a load, rebuild or
(with `--check`) restore has seeded the new physical, before the alias moves. Mirrors
sql-yodeler's rebuild (copy, check the rows match, swap).

Default checks: the database is online, and its node and relationship counts are non-zero; for an
`import` build they must also equal what the import reported. Policy checks (`load.checks`) are
Cypher returning one number, with `expect` as `== n`, `>= n`, `<= n` or `between a b`.
"""

from __future__ import annotations

import re

_EXPECT = re.compile(r"^\s*(==|>=|<=|between)\s+(-?\d+(?:\.\d+)?)(?:\s+(-?\d+(?:\.\d+)?))?\s*$")


class CheckFailed(Exception):
    """A check failed; `results` has every check run so far (the failing one last)."""

    def __init__(self, msg: str, results: list[dict]):
        super().__init__(msg)
        self.results = results


def parse_expect(expect: str):
    """`expect` -> a predicate on a number. Raises ValueError for anything else."""
    m = _EXPECT.match(expect or "")
    if not m:
        raise ValueError(f"expect {expect!r}: use '== n', '>= n', '<= n' or 'between a b'")
    op, a, b = m.group(1), float(m.group(2)), m.group(3)
    if op == "between":
        if b is None:
            raise ValueError(f"expect {expect!r}: 'between' needs two numbers")
        lo, hi = sorted((a, float(b)))
        return lambda v: lo <= v <= hi
    if b is not None:
        raise ValueError(f"expect {expect!r}: one number after {op}")
    return {"==": lambda v: v == a, ">=": lambda v: v >= a, "<=": lambda v: v <= a}[op]


def import_counts(output: str) -> dict | None:
    """Node and relationship counts from `neo4j-admin database import` output, or None.

    The import summary reads, for example: `Imported:` then `  3 nodes`, `  2 relationships`,
    `  6 properties` (#118 captures the output)."""
    nodes = re.search(r"Imported:.*?(\d[\d,]*)\s+nodes", output or "", re.S)
    rels = re.search(r"Imported:.*?(\d[\d,]*)\s+relationships", output or "", re.S)
    if not nodes:
        return None
    n = int(nodes.group(1).replace(",", ""))
    r = int(rels.group(1).replace(",", "")) if rels else 0
    return {"nodes": n, "relationships": r}


def _one_number(rows) -> float:
    if not rows or len(rows[0]) != 1:
        raise ValueError("a check query must return one row with one value")
    v = next(iter(rows[0].values()))
    if not isinstance(v, (int, float)):
        raise ValueError(f"a check query must return a number, got {v!r}")
    return v


def run(neo4j, database: str, checks: list | None = None, *,
        expect_counts: dict | None = None, require_data: bool = True) -> list[dict]:
    """Run the default checks, then each policy check, against `database`. Returns the results
    ({name, value, expect, ok}); raises CheckFailed at the first failure."""
    results: list[dict] = []

    def record(name, value, expect, ok):
        results.append({"name": name, "value": value, "expect": expect, "ok": ok})
        if not ok:
            raise CheckFailed(f"check {name!r} failed on {database}: got {value!r}, "
                              f"expected {expect}", results)

    status = neo4j.run_system("SHOW DATABASE $d YIELD currentStatus RETURN currentStatus",
                              d=database)
    st = status[0]["currentStatus"] if status else None
    record("online", st, "online", st == "online")
    nodes = _one_number(neo4j.run_on(database, "MATCH (n) RETURN count(n) AS n"))
    rels = _one_number(neo4j.run_on(database, "MATCH ()-[r]->() RETURN count(r) AS n"))
    if require_data:
        record("nodes", nodes, "> 0", nodes > 0)
    if expect_counts:
        record("nodes match import", nodes, f"== {expect_counts['nodes']}",
               nodes == expect_counts["nodes"])
        record("relationships match import", rels, f"== {expect_counts['relationships']}",
               rels == expect_counts["relationships"])
    for i, c in enumerate(checks or []):
        query, expect = (c["query"], c["expect"]) if isinstance(c, dict) else (c.query, c.expect)
        value = _one_number(neo4j.run_on(database, query))
        record(c.get("name") if isinstance(c, dict) and c.get("name") else
               getattr(c, "name", None) or f"check {i + 1}", value, expect,
               parse_expect(expect)(value))
    return results
