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


def unmanaged_prefixes(store, layout, policy) -> list[str]:
    """Top-level folders in the policy's stores that belong to no group (#124): a group removed
    from the policy without being retired. Reported, never deleted. Folders starting with `_`
    are the tool's own (`_dbms/`, `_state/`, `_verify/`, ...)."""
    known = {layout.alias_prefix(g.id, a).split("/", 1)[0] + "/"
             for g in policy.db_groups for a in g.names}
    store_name = getattr(policy, "store_name", None)
    names = ({store_name(g.id) for g in policy.db_groups} if store_name else set()) | {"default"}
    out = set()
    for name in sorted(names):
        try:
            st = store.named(name) if hasattr(store, "named") else store
            for p in st.list_prefixes(""):
                if not p.startswith("_") and p not in known:
                    out.add(p if name == "default" else f"{name}:{p}")
        except Exception:  # noqa: BLE001 - a store without listings, or unreachable
            continue
    return sorted(out)


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
    retired = getattr(group, "retired", False)
    reason = None if retired else _by_design(group, neo4j, alias)
    if retired:  # offboarded (#124): no schedules; its data goes on deletes_on
        state = "retired"
    elif reason:
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
            **({"reason": reason} if reason else {}),
            **({"deletes_on": group.deletes_on().isoformat()} if retired else {})}


def status(store, layout, groups, *, neo4j=None, now: datetime | None = None,
           policy=None) -> dict:
    """Every target's freshness; with `policy`, also the retired groups and when their data
    goes, and the store folders no group owns (#124)."""
    now = now or datetime.now(timezone.utc)
    rows = [member_status(store, layout, g, a, now=now, neo4j=neo4j)
            for g in groups for a in g.names]
    out = {"targets": rows, "stale": [r["target"] for r in rows if r["state"] == "stale"]}
    if policy is not None:
        out["retired"] = [{"group": g.id, "retired_on": g.retired_on.isoformat(),
                           "deletes_on": g.deletes_on().isoformat()}
                          for g in groups if getattr(g, "retired", False)]
        out["unmanaged"] = unmanaged_prefixes(store, layout, policy)
    return out
