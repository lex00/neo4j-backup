"""#113: restore points per member, read-only, from sidecars and load folders."""

import json

from neo4j_backup_core import artifacts, paths, points
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore, rec

L = paths.DefaultPathLayout()
POL = Policy.model_validate({
    "db_groups": [{"id": "g", "aliases": ["orders"], "tier": "gold"}],
    "tiers": {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}})


def put(s, folder, file, full, when, dbid):
    key = folder + file
    s.add(key)
    s.put_text(artifacts.sidecar_key(key), json.dumps(
        {**artifacts._normalise(rec(file, full, 1, 5, when, folder=folder, dbid=dbid)), "key": key}))


def store():
    s = MemStore()
    put(s, "g/orders/orders-1/", "1.backup", True, "2026-11-15T01:00:00", "A")
    put(s, "g/orders/orders-1/", "2.backup", False, "2026-11-15T02:00:00", "A")
    put(s, "g/orders/orders-1/", "3.backup", False, "2026-11-15T09:00:00", "A")  # 7 h gap
    put(s, "g/orders/orders-2/", "4.backup", True, "2026-11-15T10:00:00", "B")
    s.add("g/orders/_seed/20261115t000000/orders.dump")
    return s


class Neo:
    def __init__(self, current):
        self.current = current

    def resolve_physical(self, a):
        return self.current


def test_two_physicals_a_load_and_a_gap():
    out = points.restore_points(store(), L, POL.group("g"), neo4j=Neo("orders-2"), policy=POL)
    m = out["members"][0]
    assert m["current"] == "orders-2" and m["latest"] == "2026-11-15T10:00:00"
    assert [(c["physical"], c["from"], c["to"]) for c in m["chains"]] == [
        ("orders-1", "2026-11-15T01:00:00", "2026-11-15T09:00:00"),
        ("orders-2", "2026-11-15T10:00:00", "2026-11-15T10:00:00")]
    assert m["loads"][0]["restore"] == \
        "restore g --from-dump orders=g/orders/_seed/20261115t000000/orders.dump"
    assert [g["kind"] for g in m["gaps"]] == ["diffs missing"]
    assert m["chains"][0]["restore"].startswith("restore g --until")


def test_no_chain_since_the_last_swap():
    m = points.restore_points(store(), L, POL.group("g"), neo4j=Neo("orders-3"),
                              policy=POL)["members"][0]
    assert any(g["kind"] == "no chain since the last swap" for g in m["gaps"])


def test_works_without_neo4j():
    m = points.restore_points(store(), L, POL.group("g"), policy=POL)["members"][0]
    assert m["current"] is None and len(m["chains"]) == 2
