"""Restore points (#113): what a group can be restored to, per member, read from the artifact
sidecars (#118) and the load folders (#156). Read-only: no neo4j-admin, and Neo4j only to name
the database each alias serves now (skipped when no client is given).
"""

from __future__ import annotations

from datetime import timedelta

from . import artifacts, cron
from .ops import _sidecar_time, _store


def _diff_interval(policy, group) -> timedelta | None:
    tier = policy.tiers.get(group.tier) if policy is not None else None
    if tier is None or not tier.diff_cron:
        return None
    try:
        return cron.max_gap(tier.diff_cron)
    except ValueError:
        return None


def member_points(store, layout, group, name: str, *, neo4j=None, policy=None) -> dict:
    store = _store(store, group.id)
    read = artifacts.chains(store, layout.alias_prefix(group.id, name))
    current = None
    if neo4j is not None:
        try:
            current = (neo4j.for_group(group.id) if hasattr(neo4j, "for_group")
                       else neo4j).resolve_physical(name)
        except Exception:  # noqa: BLE001 - read-only view: Neo4j being down isn't fatal
            current = None
    loads = [{"key": k, "kind": kind, "time": m.isoformat() if hasattr(m, "isoformat") else m,
              "restore": f"restore {group.id} --from-dump {name}={k}"}
             for (k, m, kind) in store.list_load_artifacts(layout.load_prefix(group.id, name))]
    interval = _diff_interval(policy, group)
    chains, gaps = [], []
    for c in sorted(read["chains"], key=lambda c: c["from"] or ""):
        entry = {"physical": c["folder"].rstrip("/").rsplit("/", 1)[-1], "from": c["from"],
                 "to": c["to"], "artifacts": len(c["artifacts"]),
                 "restore": f"restore {group.id} --until <time between from and to>"}
        if c.get("broken"):
            gaps.append({"kind": "broken chain", "physical": entry["physical"],
                         "detail": c["broken"]})
        if interval:
            times = [_sidecar_time(a["time"]) for a in c["artifacts"] if a.get("time")]
            for a, b in zip(times, times[1:]):
                if b - a > 2 * interval:
                    gaps.append({"kind": "diffs missing", "physical": entry["physical"],
                                 "from": a.isoformat(), "to": b.isoformat(),
                                 "detail": f"no backup for {b - a} (the tier backs up every "
                                           f"{interval})"})
        chains.append(entry)
    if current and not any(c["physical"] == current for c in chains):
        gaps.append({"kind": "no chain since the last swap", "physical": current,
                     "detail": "the database the alias serves has no backups yet; the newest "
                               "point is before the swap"})
    latest = chains[-1]["to"] if chains else None
    return {"member": name, "current": current, "latest": latest,
            "latest_restore": f"restore {group.id}" if chains or loads else None,
            "chains": chains, "loads": loads, "gaps": gaps, "unknown": read["unknown"]}


def restore_points(store, layout, group, *, neo4j=None, policy=None) -> dict:
    """Every member's restore points (#113). See `member_points`."""
    return {"group": group.id, "tenant": getattr(group, "tenant", None),
            "members": [member_points(store, layout, group, n, neo4j=neo4j, policy=policy)
                        for n in group.names]}
