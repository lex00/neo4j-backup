"""Bring existing databases under the alias model (#92).

A database and an alias can't share a name, so there are two cases per member:

- `overrides.<alias>.adopt_from: <existing-db>` (a different name): create the alias for the
  existing database. Nothing is copied or dropped.
- The database already has the member's name (a same-name migration, destructive): back it up,
  verify the backup, seed a new physical from it, check it, copy its privileges and `writes`
  setting, DROP the original, then create the alias. Needs `migrate` and `replace`.

The plan is checked whole before any change: an alias that already points somewhere else is a
conflict and nothing happens. Each adopted target is recorded in `_state/<group>.json` like any
swap (#122), so rollback and cleanup see it. alias-swap groups only.
"""

from __future__ import annotations

from . import naming, ops

_NOLOG = ops._NOLOG


def plan_adopt(neo4j, group, *, migrate: bool = False, replace: bool = False) -> dict:
    """Per member: `create` (alias for an existing database), `skip` (the alias already targets
    it), `conflict` (the alias points elsewhere), `migrate` (same-name, destructive),
    `missing` (nothing to adopt)."""
    if group.restore_mode != "alias-swap":
        raise ops.OpError(f"{group.id} is a by-name group: there is no alias to adopt into")
    neo = ops._per(neo4j, group.id)
    members = []
    for name in group.names:
        src = (getattr(group, "overrides", {}) or {}).get(name, {}).get("adopt_from")
        current = neo.alias_target(name)
        if src and src != name:
            if current == src:
                action = "skip"
            elif current is not None:
                action = "conflict"
            elif not neo.database_exists(src):
                action = "missing"
            else:
                action = "create"
            members.append({"alias": name, "database": src, "current": current,
                            "action": action})
        elif current is not None:
            members.append({"alias": name, "database": current, "current": current,
                            "action": "skip"})
        elif neo.database_exists(name):
            members.append({"alias": name, "database": name, "current": None,
                            "action": "migrate",
                            "privileges": neo.privilege_commands(name),
                            "drops": [name]})
        else:
            members.append({"alias": name, "database": src, "current": None,
                            "action": "missing"})
    problems = [m for m in members if m["action"] in ("conflict", "missing")]
    out = {"group": group.id, "members": members,
           "drops": [d for m in members for d in m.get("drops", [])],
           "problems": problems}
    if any(m["action"] == "migrate" for m in members) and not (migrate and replace):
        out["needs"] = ("same-name members are migrated: back up, seed a new physical, DROP "
                        "the original, create the alias. Pass migrate and replace")
    return out


def adopt(run_admin, neo4j, store, runner, layout, group, *, migrate: bool = False,
          replace: bool = False, upload: str = "admin", staging: str | None = None,
          log=_NOLOG) -> dict:
    plan = plan_adopt(neo4j, group, migrate=migrate, replace=replace)
    if plan["problems"]:
        raise ops.OpError("nothing adopted: " + "; ".join(
            f"{m['alias']}: {m['action']}" + (f" (points at {m['current']})"
                                              if m["action"] == "conflict" else "")
            for m in plan["problems"]))
    if plan.get("needs"):
        raise ops.OpError(f"nothing adopted: {plan['needs']}")
    neo, st = ops._per(neo4j, group.id), ops._store(store, group.id)
    swaps = []
    for m in plan["members"]:
        if m["action"] == "create":
            neo.create_alias(m["alias"], m["database"])
            log(f"alias {m['alias']} -> {m['database']}")
            swaps.append({"alias": m["alias"], "old": None, "newdb": m["database"]})
        elif m["action"] == "migrate":
            m.update(_migrate(run_admin, neo4j, store, runner, layout, group, m["alias"],
                              upload=upload, staging=staging, log=log))
            swaps.append({"alias": m["alias"], "old": None, "newdb": m["newdb"]})
    if swaps:
        ops.record_swaps(st, group.id, swaps)
    return plan


def _migrate(run_admin, neo4j, store, runner, layout, group, name: str, *, upload: str,
             staging: str | None, log) -> dict:
    """Same-name migration of one member. Every step before the DROP can fail without loss."""
    neo = ops._per(neo4j, group.id)
    backup = ops.backup_target(run_admin, neo4j, store, runner, layout, group.id, name, "FULL",
                               upload=upload, staging=staging, log=log, physical=name)
    verified = ops.verify_target(run_admin, store, runner, layout, group.id, name,
                                 upload=upload, staging=staging, log=log)
    if not verified.get("consistent"):
        raise ops.OpError(f"{name}: the backup didn't verify; nothing dropped")
    st = ops._store(store, group.id)
    newdb = naming.scheme().physical(name, naming.ts(), None)
    key = backup["artifact"]
    neo.seed_database(newdb, st.uri(key), topology=ops._topology(neo, group, name),
                      cypher_version=ops._cypher(neo, None), store_base=st.uri(""))
    log(f"seeded {newdb} <= {key}")
    entry = {"mode": "alias-swap", "alias": name, "newdb": newdb, "old": None,
             "group": group.id}
    ops.check_seeded(neo, group, [entry], require_data=False, log=log)
    privileges = ops.regrant(neo, neo.privilege_commands(name), old=name, new=newdb, log=log)
    ops.apply_access(neo, group, newdb, log=log)
    neo.drop_database(name)
    log(f"dropped the original {name}")
    neo.create_alias(name, newdb)
    log(f"alias {name} -> {newdb}")
    return {"newdb": newdb, "backup": key, "privileges": privileges}
