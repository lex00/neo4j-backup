"""The MCP tool *logic* — pure functions over `neo4j_backup_core`, no `mcp` dependency, so they run
(and are tested) without the SDK installed. `server.py` wraps these as MCP tools.

Two tiers:

- **Read-only** (`list_targets`, `latest_artifact`, `show_chain`, `backup_status`, `preview_restore`,
  `preview_prune`) — safe status/planning over the policy + object store. Most of the operator value.
- **Mutating** (`run_backup`, `run_verify`, `run_aggregate`, `run_restore`, `run_prune`) — each
  returns a `needs_confirmation` result (with the blast radius) unless called with `confirm=True`;
  destructive `run_restore(replace=True)` runs **verify-before-drop** first. `server.py` only exposes
  these when the server is started read-write.
"""

from __future__ import annotations

import functools
import os
import sys
import time

from neo4j_backup_core import env, ops, paths, record
from neo4j_backup_core.policy import load_policy
from neo4j_backup_core.until import parse_until

_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
_STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None
_CYPHER = os.environ.get("SEED_CYPHER_VERSION") or None


def _layout():
    return paths.get_layout()


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)  # stdout is the MCP stdio channel


def _recorded(fn):
    """Check and log `ticket` / `reason` (#119) and return them with the tool's result."""
    @functools.wraps(fn)
    def wrapper(*args, ticket: str | None = None, reason: str | None = None, **kw):
        rec = record.fields(ticket, reason,
                            applying=bool(kw.get("confirm")) and not kw.get("dry_run"))
        _log(record.describe(fn.__name__, rec))
        out = fn(*args, ticket=ticket, reason=reason, **kw)
        return {**out, **rec} if isinstance(out, dict) else out
    return wrapper


def _selectable(fn):
    """`group` or `tenant` (#102): with a tenant, run `fn` for every group carrying that label and
    return {"tenant", "groups": [...]}. Both, neither or an unknown name raises SelectionError."""
    @functools.wraps(fn)
    def wrapper(*args, group: str | None = None, tenant: str | None = None, **kw):
        groups = load_policy(env.policy_path()).select(group, tenant)
        if not tenant:
            return fn(*args, group=group, tenant=None, **kw)
        return {"tenant": tenant,
                "groups": [fn(*args, group=g.id, tenant=None, **kw) for g in groups]}
    return wrapper


def _run_admin(runner):
    # neo4j-admin output to stderr so it never corrupts the MCP stdio protocol on stdout
    return env.subprocess_admin(runner, stdout=sys.stderr)


def _iso(m):
    return m.isoformat() if hasattr(m, "isoformat") else str(m)


def _needs_confirmation(op, blast):
    return {"ok": False, "needs_confirmation": True, "op": op,
            "message": f"{op} mutates state — call again with confirm=true to apply, "
                       f"or dry_run=true to preview only",
            **blast}


# --- read-only -------------------------------------------------------------------------------

def list_targets(group: str | None = None, tenant: str | None = None) -> dict:
    """List the backup targets (group/member) the policy covers, optionally only one group's or
    one tenant's (#102), with each group's tenant label."""
    policy = load_policy(env.policy_path())
    groups = policy.select(group, tenant) if (group or tenant) else policy.db_groups
    return {"targets": [f"{g.id}/{n}" for g in groups for n in g.names],
            "tenants": {g.id: g.tenant for g in groups},
            "clusters": {g.id: getattr(g, "cluster", None) or "default" for g in groups}}


def latest_artifact(group: str, member: str) -> dict:
    """The newest backup artifact for one group member (its chain head), or null if none."""
    store, layout = env.store().for_group(group), _layout()  # the group's store (#105)
    key = store.latest_artifact_key(layout.alias_prefix(group, member))
    if not key:
        return {"group": group, "member": member, "artifact": None}
    return {"group": group, "member": member, "artifact": key, "bytes": store.object_size(key)}


def show_chain(group: str, member: str) -> dict:
    """List the backup artifacts for one group member, oldest first, and the chains read from
    their sidecars (#118): per physical folder, full then diffs, with the time window each covers.
    Artifacts with no sidecar yet are listed under `unknown`."""
    store, layout = env.store().for_group(group), _layout()  # the group's store (#105)
    arts = sorted(store.list_artifacts(layout.alias_prefix(group, member)), key=lambda t: t[2])
    chain = [{"key": k, "bytes": s, "modified": _iso(m)} for (k, s, m) in arts]
    read = ops.artifact_chains(store, layout, group, member)
    return {"group": group, "member": member, "count": len(chain), "artifacts": chain,
            "chains": read["chains"], "unknown": read["unknown"],
            # load artifacts (#156), separate from the backup chains
            "loads": ops.load_artifacts(env.store(), layout, group, member)}


def backup_status(tenant: str | None = None) -> dict:
    """Freshness of every target (or one tenant's, #102): its latest artifact and how many hours
    old it is (null if never backed up), with the group's tenant. The 'are my backups current?'
    view."""
    store, policy, layout = env.store(), load_policy(env.policy_path()), _layout()
    now = time.time()
    rows = []
    for g in (policy.select(tenant=tenant) if tenant else policy.db_groups):
        for a in g.names:
            arts = ops._store(store, g.id).list_artifacts(layout.alias_prefix(g.id, a))
            if not arts:
                rows.append({"target": f"{g.id}/{a}", "tenant": g.tenant,
                             "cluster": getattr(g, "cluster", None) or "default",
                             "latest": None, "age_hours": None})
                continue
            k, _s, m = max(arts, key=lambda t: t[2])
            age = round((now - m.timestamp()) / 3600, 1) if hasattr(m, "timestamp") else None
            loads = ops.load_artifacts(store, layout, g.id, a)
            rows.append({"target": f"{g.id}/{a}", "tenant": g.tenant,
                         "cluster": getattr(g, "cluster", None) or "default", "latest": k,
                         "age_hours": age,
                         "latest_load": loads[-1]["key"] if loads else None})
    return {"targets": rows}


def preview_restore(group: str, replace: bool = False, until: str | None = None) -> dict:
    """The blast radius of restoring a group WITHOUT mutating: per member, the artifact and what
    would happen (by-name create / drop+recreate, or alias-swap seed + swap). Runs preconditions.
    With `until`, each member's `seed` names the backup chain and window that covers it (#114)."""
    until = parse_until(until)
    g = load_policy(env.policy_path()).group(group)
    return {"group": group, "plan": ops.plan_restore(env.neo4j(), env.store(), g, _layout(),
                                                      replace=replace, until=until)}


def preview_prune() -> dict:
    """The artifacts retention would delete right now, WITHOUT deleting them. Whole chains only
    (#73); `kept_for_chains` counts artifacts past retention kept because newer ones need them,
    `unknown_kept` those with no metadata sidecar yet (kept; run_prune fills them in first)."""
    out = ops.prune(env.store(), load_policy(env.policy_path()), _layout(), dry_run=True)
    return {k: out[k] for k in ("deleted", "detail", "keys", "kept_for_chains", "unknown_kept", "loads")}


# --- mutating (read-write scope only; each guarded by confirm) --------------------------------

@_selectable
def run_backup(group: str | None = None, kind: str = "AUTO", tenant: str | None = None) -> dict:
    """Back up every database in a group (additive — writes new artifacts). `kind` AUTO|FULL|DIFF."""
    g = load_policy(env.policy_path()).group(group)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), _layout()
    run = _run_admin(runner)
    done = [ops.backup_target(run, neo, store, runner, layout, g.id, m, kind,
                              upload=_UPLOAD, staging=_STAGING, log=_log, group=g)
            for m in g.names]
    return {"group": group, "kind": kind, "backups": done}


@_selectable
def run_verify(group: str | None = None, tenant: str | None = None) -> dict:
    """Non-destructively check that the latest backups of a group are restorable and consistent."""
    g = load_policy(env.policy_path()).group(group)
    store, runner, layout = env.store(), env.runner(), _layout()
    run = _run_admin(runner)
    done = [ops.verify_target(run, store, runner, layout, g.id, m,
                              upload=_UPLOAD, staging=_STAGING) for m in g.names]
    return {"group": group, "verified": done}


@_recorded
@_selectable
def run_aggregate(group: str | None = None, confirm: bool = False, dry_run: bool = False,
                  ticket: str | None = None, reason: str | None = None,
                  tenant: str | None = None) -> dict:
    """Collapse each of a group's chains into one recovered full, in place (destructive to the
    chain: trades intra-chain PITR). Needs confirm=true; dry_run previews the physicals affected."""
    g = load_policy(env.policy_path()).group(group)
    store, runner, layout = env.store(), env.runner(), _layout()
    plan = [{"member": m, "physical": ops.head_physical(store, layout, g.id, m)[1]} for m in g.names]
    if dry_run:
        return {"group": group, "dry_run": True, "collapses": plan}
    if not confirm:
        return _needs_confirmation("aggregate", {"collapses": plan})
    run = _run_admin(runner)
    done = [ops.aggregate_target(run, store, runner, layout, g.id, m,
                                 upload=_UPLOAD, staging=_STAGING, log=_log) for m in g.names]
    return {"group": group, "aggregated": done}


@_recorded
def run_restore(group: str, until: str | None = None, replace: bool = False,
                confirm: bool = False, dry_run: bool = False, verify_first: bool = True,
                baseline: bool = True, ticket: str | None = None,
                reason: str | None = None, from_load: bool = False,
                from_dump: dict[str, str] | None = None) -> dict:
    """Restore a group (alias-swap, or by-name with `replace` to DROP+recreate; `until` = PITR).
    Needs confirm=true; dry_run returns the plan. For a destructive `replace`, runs
    verify-before-drop (skip with verify_first=false). `until` is ISO-8601 (UTC if no offset);
    anything else is refused before any Neo4j call (#77). `baseline` (default true) takes a FULL
    of each restored database afterwards (#97); if that fails the restore stays applied and the
    result has `baseline_error`."""
    until = parse_until(until)
    g = load_policy(env.policy_path()).group(group)
    store, neo, layout = env.store(), env.neo4j(), _layout()
    loads = from_dump or ("latest" if from_load else None)  # load artifacts (#144, #159)
    plan = ops.plan_restore(neo, store, g, layout, replace=replace, until=until,
                            loads=loads)  # preconditions first, incl. the chain for `until`
    plan["baseline"] = baseline
    if dry_run:
        return {"group": group, "dry_run": True, "plan": plan}
    if not confirm:
        return _needs_confirmation("restore", {"plan": plan})
    verified = False
    if replace and plan["drops"] and verify_first:
        runner = env.runner()
        run = _run_admin(runner)
        for m in plan["drops"]:  # prove each to-be-DROPPED db's backup restores BEFORE dropping it
            ops.verify_target(run, store, runner, layout, g.id, m, upload=_UPLOAD, staging=_STAGING)
        verified = True
    out = ops.restore_group(neo, store, g, layout, restore_until=until, replace=replace,
                            cypher_version=_CYPHER, loads=loads)
    result = {"group": group, "verified_before_drop": verified, "restored": out}
    if baseline:
        runner = env.runner()
        try:
            result["baseline"] = ops.baseline_after_restore(
                _run_admin(runner), neo, store, runner, layout, g, out["members"],
                upload=_UPLOAD, staging=_STAGING, log=_log)
        except ops.BaselineError as e:
            result.update({"ok": False, "baseline": e.done,
                           "error": {"kind": e.kind, "msg": str(e)}})
    return result


@_recorded
def run_prune(confirm: bool = False, dry_run: bool = False, ticket: str | None = None,
              reason: str | None = None) -> dict:
    """Delete backups past each group's retention (destructive). Needs confirm=true; dry_run (and
    the pre-confirmation result) return the exact keys that would be deleted."""
    store, policy, layout = env.store(), load_policy(env.policy_path()), _layout()
    runner = env.runner()
    fill = {"run_admin": _run_admin(runner), "runner": runner, "log": _log}
    preview = ops.prune(store, policy, layout, dry_run=True, **fill)
    shown = {k: preview[k] for k in ("deleted", "detail", "keys", "kept_for_chains",
                                     "unknown_kept", "loads")}
    if dry_run:
        return {"dry_run": True, **shown}
    if not confirm:
        return _needs_confirmation("prune", shown)
    out = ops.prune(store, policy, layout, **fill)
    return {"deleted": out["deleted"], "detail": out["detail"],
            "kept_for_chains": out["kept_for_chains"], "unknown_kept": out["unknown_kept"]}


@_recorded
def run_load(group: str, confirm: bool = False, dry_run: bool = False, replace: bool = False,
             keep_failed: bool = False, baseline: bool = True, reapply_access: bool = False,
             ticket: str | None = None, reason: str | None = None) -> dict:
    """Load or rebuild a group from its policy build (#107): build in the runner, dump, seed a
    new database, check it, set its access, swap the alias, take the baseline. Needs
    confirm=true; dry_run returns the plan. `reapply_access` only applies the policy's `writes`
    to the current databases."""
    from neo4j_backup_core import load as load_mod

    g = load_policy(env.policy_path()).group(group)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), _layout()
    if reapply_access:
        if not confirm:
            return _needs_confirmation("load", {"reapply_access": g.load.writes})
        return {"group": group, "access": load_mod.reapply_access(neo, g, log=_log)}
    plan = load_mod.plan_load(neo, store, layout, g, replace=replace)
    if dry_run:
        return {"group": group, "dry_run": True, "plan": plan}
    if not confirm:
        return _needs_confirmation("load", {"plan": plan})
    try:
        return load_mod.load_group(_run_admin(runner), runner, neo, store, layout, g,
                                   replace=replace, keep_failed=keep_failed,
                                   baseline=None if baseline else False, upload=_UPLOAD,
                                   staging=_STAGING, cypher_version=_CYPHER, log=_log)
    except load_mod.LoadError as e:
        return {"ok": False, "error": {"kind": "load_failed", "msg": str(e)},
                "failed_step": e.step, **e.result}


READ_TOOLS = [list_targets, latest_artifact, show_chain, backup_status,
              preview_restore, preview_prune]
WRITE_TOOLS = [run_backup, run_verify, run_aggregate, run_restore, run_prune, run_load]
