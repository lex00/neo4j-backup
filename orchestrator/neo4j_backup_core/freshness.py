"""Freshness against `rpo_minutes` (#123).

Each member's newest restorable point is its newest backup or load artifact. A member is:

- `ok`: that point is at most `rpo_minutes` old;
- `stale`: it's older, or there's none at all;
- `by_design`: it isn't backed up by design (#112): `writes: read-only` (its loads restore it),
  or read-write with `protect_writes: false`. With a Neo4j handle, a database that is read-only
  live counts too, as it does for the backup lane.

Read-only: the store's listings, and Neo4j only when given (and only to read access modes).
"""

from __future__ import annotations

from datetime import datetime, timezone

from . import ops


def _by_design(group, neo4j, alias: str) -> str | None:
    load = getattr(group, "load", None)
    writes = getattr(load, "writes", None)
    if writes == "read-only":
        return "writes: read-only (its loads restore it)"
    if writes == "read-write" and getattr(load, "protect_writes", None) is False:
        return "protect_writes: false (writes are disposable)"
    if neo4j is not None:
        try:
            neo = ops._per(neo4j, group.id)
            physical = neo.resolve_physical(alias)
            if physical and not ops.backs_up(neo, group, physical)["backs_up"]:
                return "the database is read-only (its loads restore it)"
        except Exception:  # noqa: BLE001 - Neo4j is optional here; the policy decides
            pass
    return None


def _when(m) -> datetime | None:
    if isinstance(m, datetime):
        return m if m.tzinfo else m.replace(tzinfo=timezone.utc)
    if isinstance(m, str):
        try:
            return _when(datetime.fromisoformat(m.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def member_status(store, layout, group, alias: str, *, now: datetime,
                  neo4j=None) -> dict:
    st = ops._store(store, group.id)
    arts = st.list_artifacts(layout.alias_prefix(group.id, alias))
    latest = max(arts, key=lambda t: t[2]) if arts else None
    try:
        loads = ops.load_artifacts(store, layout, group.id, alias)
    except Exception:  # noqa: BLE001 - a store without load listings
        loads = []
    points = [p for p in ([_when(latest[2])] if latest else []) +
              [_when(x["modified"]) for x in loads] if p is not None]
    newest = max(points) if points else None
    age_min = round((now - newest).total_seconds() / 60, 1) if newest else None
    rpo = getattr(group, "rpo_minutes", None)
    reason = _by_design(group, neo4j, alias)
    if reason:
        state = "by_design"
    elif newest is None or (rpo is not None and age_min > rpo):
        state = "stale"
    else:
        state = "ok"
    return {"target": f"{group.id}/{alias}", "tenant": getattr(group, "tenant", None),
            "cluster": getattr(group, "cluster", None) or "default",
            "latest": latest[0] if latest else None,
            "age_hours": round((now - _when(latest[2])).total_seconds() / 3600, 1)
            if latest and _when(latest[2]) else None,
            "latest_load": loads[-1]["key"] if loads else None,
            "newest_point": newest.isoformat() if newest else None,
            "age_minutes": age_min, "rpo_minutes": rpo, "state": state,
            **({"reason": reason} if reason else {})}


def status(store, layout, groups, *, neo4j=None, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    rows = [member_status(store, layout, g, a, now=now, neo4j=neo4j)
            for g in groups for a in g.names]
    return {"targets": rows, "stale": [r["target"] for r in rows if r["state"] == "stale"]}
