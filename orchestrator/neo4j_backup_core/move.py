"""Restore a group into another cluster (#115): a lost cluster, a region failover, or a move.

Seeding reads a URI, so any cluster whose servers can read the group's store can restore it. The
steps, all checked before anything changes:

1. The target is a cluster in the policy and answers; the source is either unreachable, or the
   operator passed `fence_source` (so two clusters never back up one group into one chain).
2. Every member has a seed (the same choice as a restore, `until` included).
3. The source's users, roles and DBMS-wide privileges: live from the source when it answers, else
   from its newest metadata export. Database-scoped privileges are taken only for the moved
   members, rewritten from the source's physical to the new one (#117). Alias lines are not
   replayed; the cutover creates or swaps each alias on the target.

Then: replay the security part on the target, seed every member (a seed the target's servers
can't read fails here, before any alias moves), grant the members' privileges, create or swap the
aliases, and fence the group on the source (`_state/<group>.json`): backups, restores and loads
that resolve to the source cluster refuse until the policy's `cluster:` names the target. The
policy is never edited. The caller takes the baseline backups on the target.
"""

from __future__ import annotations

import re

from . import metadata, naming, ops, privileges

_NOLOG = ops._NOLOG
_ALIAS_RE = re.compile(r"^CREATE ALIAS (`(?:[^`]|``)+`|\S+) IF NOT EXISTS FOR DATABASE "
                       r"(`(?:[^`]|``)+`|\S+)$")
_NAMED_DB = re.compile(r" ON (GRAPH|DATABASE)S? `")


def _unquote(s: str) -> str:
    return s[1:-1].replace("``", "`") if s.startswith("`") else s


def split_metadata(text: str) -> dict:
    """A rendered metadata export, split for a move: `security` (roles, users, memberships and
    privileges that name no particular database), `scoped` (privileges on a named database) and
    `aliases` ({alias: database} for local aliases)."""
    out = {"security": [], "scoped": [], "aliases": {}}
    for stmt in metadata.statements(text):
        m = _ALIAS_RE.match(stmt)
        if m:
            out["aliases"][_unquote(m.group(1))] = _unquote(m.group(2))
        elif stmt.startswith("CREATE ALIAS"):
            continue  # remote aliases: not part of a group move
        elif _NAMED_DB.search(stmt):
            out["scoped"].append(stmt)
        else:
            out["security"].append(stmt)
    return out


def _reachable(neo) -> bool:
    try:
        neo.list_databases()
        return True
    except Exception:  # noqa: BLE001 - any failure counts as unreachable
        return False


def _source_metadata(neo4j, store, layout, source: str, reachable: bool) -> tuple[str, str]:
    """(text, where-from): live when the source answers, else its newest export."""
    if reachable:
        src = ops._cluster(neo4j, source)
        return metadata.render(metadata.capture(src), ts=naming.ts()), "live"
    st = ops._cluster(store, source)
    key = st.latest_text_key(layout.metadata_listing(source))
    if not key:
        raise ops.OpError(f"cluster {source!r} is unreachable and has no metadata export: the "
                          f"target needs its users and roles first. Replay them yourself and "
                          f"pass skip_metadata (CLI --no-metadata)")
    return st.get_text(key), key


def public(plan: dict) -> dict:
    """The plan without its internal fields (the replayed statements carry placeholder
    passwords)."""
    return {k: v for k, v in plan.items() if not k.startswith("_")}


def groups_for(policy, group_id: str | None, from_cluster: str | None) -> tuple[list, str]:
    """(groups, source cluster): one group (its policy cluster), or every group of
    `from_cluster`, or that one group if it is on `from_cluster`."""
    if from_cluster:
        groups = [g for g in policy.db_groups if policy.cluster_name(g.id) == from_cluster
                  and (group_id is None or g.id == group_id)]
        if group_id and not groups:
            raise ops.OpError(f"{group_id} is not on cluster {from_cluster!r}")
        return groups, from_cluster
    if not group_id:
        raise ops.OpError("name a group, or --from-cluster for every group of a cluster")
    return [policy.group(group_id)], policy.cluster_name(group_id)


def baseline(run_admin, neo4j, store, runner, layout, groups: list, plan: dict, *,
             upload: str = "admin", staging: str | None = None, log=_NOLOG) -> list[dict]:
    """A FULL of every database the move created, against the target cluster (#97)."""
    tgt, trun = ops._cluster(neo4j, plan["target"]), ops._cluster(runner, plan["target"])
    by_id = {g.id: g for g in groups}
    done = []
    for gp in plan["groups"]:
        done += ops.baseline_after_restore(run_admin, tgt, store, trun, layout,
                                           by_id[gp["group"]], gp["seeded"], upload=upload,
                                           staging=staging, log=log)
    return done


def plan_move(neo4j, store, layout, groups: list, source: str, target: str, *,
              until: str | None = None, fence_source: bool = False,
              skip_metadata: bool = False, clusters: list[str] | None = None) -> dict:
    """Everything `move` would do, after every check, without changing anything."""
    if source == target:
        raise ops.OpError(f"the group is already on cluster {target!r}")
    if clusters is not None and target not in clusters:
        raise ops.OpError(f"cluster {target!r} is not in the policy (clusters: {clusters})")
    if not groups:
        raise ops.OpError(f"no groups on cluster {source!r}")
    tgt = ops._cluster(neo4j, target)
    if not _reachable(tgt):
        raise ops.OpError(f"the target cluster {target!r} doesn't answer")
    reachable = _reachable(ops._cluster(neo4j, source))
    if reachable and not fence_source:
        raise ops.OpError(
            f"the source cluster {source!r} still answers: pass fence_source (CLI "
            f"--fence-source) to stop the groups' backups and loads there, so two clusters "
            f"don't write one chain")
    meta = {"security": [], "scoped": [], "aliases": {}}
    meta_from = None
    if not skip_metadata:
        text, meta_from = _source_metadata(neo4j, store, layout, source, reachable)
        meta = split_metadata(text)
    src = ops._cluster(neo4j, source) if reachable else None
    out = []
    for group in groups:
        st = ops._store(store, group.id)
        seeds = ops.choose_seeds(st, layout, group, until)
        members = []
        for name in group.names:
            by_name = group.restore_mode == "by-name"
            old_src = name if by_name else (src.alias_target(name) if src is not None
                                            else meta["aliases"].get(name))
            scoped = (src.privilege_commands(old_src) if src is not None and old_src
                      else privileges.scoped_to(meta["scoped"], old_src) if old_src else [])
            members.append({"alias": name, "mode": group.restore_mode, "seed": seeds[name],
                            "source_physical": old_src, "privileges": scoped,
                            "target_current": None if by_name else tgt.alias_target(name),
                            "action": ("create on the target" if by_name
                                       else "seed, then create or swap the alias")})
        out.append({"group": group.id, "members": members})
    return {"source": source, "target": target, "source_reachable": reachable,
            "fence_source": True, "until": until,
            "metadata": None if skip_metadata else {
                "from": meta_from, "statements": len(meta["security"])},
            "store_read": "the target's servers must be able to read each group's store; a seed "
                          "that can't be read fails before any alias moves",
            "groups": out, "_security": meta["security"],
            "policy": f"the policy is not edited: set `cluster: {target}` on "
                      f"{', '.join(g.id for g in groups)} if the move is permanent; until then "
                      f"their backups and loads on {source!r} are fenced off"}


def move(neo4j, store, layout, groups: list, source: str, target: str, *,
         until: str | None = None, fence_source: bool = False, skip_metadata: bool = False,
         clusters: list[str] | None = None, cypher_version: str | None = None,
         log=_NOLOG) -> dict:
    """Do what `plan_move` describes. Returns the plan with each group's seeded members (for the
    baseline, taken by the caller against the target)."""
    plan = plan_move(neo4j, store, layout, groups, source, target, until=until,
                     fence_source=fence_source, skip_metadata=skip_metadata, clusters=clusters)
    tgt = ops._cluster(neo4j, target)
    replayed = 0
    for stmt in plan.pop("_security"):  # users and roles before any grant names them
        tgt.run_system(stmt)
        replayed += 1
    log(f"replayed {replayed} metadata statements onto {target}")
    cypher_version = ops._cypher(tgt, cypher_version)
    by_id = {g.id: g for g in groups}
    for gp in plan["groups"]:
        group, st = by_id[gp["group"]], ops._store(store, gp["group"])
        ts = naming.ts_for(group)
        seeded = []
        for m in gp["members"]:  # every seed first: an unreadable store stops here
            seeded.append(ops.seed_member(tgt, st, group, layout, m["alias"],
                                          restore_until=m["seed"]["until"], ts=ts,
                                          cypher_version=cypher_version, log=log,
                                          seed=m["seed"]))
        for m, s in zip(gp["members"], seeded):
            new = s.get("newdb") or s.get("name")
            ops.regrant(tgt, m["privileges"], old=m["source_physical"], new=new, log=log)
        ops.cutover_seeded(tgt, seeded, log=log, store=st, group_id=group.id)
        ops.fence(st, group.id, source, target)
        log(f"{group.id}: moved to {target}; fenced on {source}")
        gp["seeded"] = seeded
    plan["metadata_replayed"] = replayed
    return plan
