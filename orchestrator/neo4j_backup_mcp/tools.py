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

import os
import sys
import time

from neo4j_backup_core import env, ops, paths
from neo4j_backup_core.policy import load_policy

_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
_STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None
_CYPHER = os.environ.get("SEED_CYPHER_VERSION") or None


def _layout():
    return paths.get_layout()


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

def list_targets() -> dict:
    """List every backup target (group/member) the policy covers."""
    return {"targets": load_policy(env.policy_path()).partition_keys()}


def latest_artifact(group: str, member: str) -> dict:
    """The newest backup artifact for one group member (its chain head), or null if none."""
    store, layout = env.store(), _layout()
    key = store.latest_artifact_key(layout.alias_prefix(group, member))
    if not key:
        return {"group": group, "member": member, "artifact": None}
    return {"group": group, "member": member, "artifact": key, "bytes": store.object_size(key)}


def show_chain(group: str, member: str) -> dict:
    """List the backup artifacts for one group member, oldest first (the restore chain)."""
    store, layout = env.store(), _layout()
    arts = sorted(store.list_artifacts(layout.alias_prefix(group, member)), key=lambda t: t[2])
    chain = [{"key": k, "bytes": s, "modified": _iso(m)} for (k, s, m) in arts]
    return {"group": group, "member": member, "count": len(chain), "artifacts": chain}


def backup_status() -> dict:
    """Freshness of every target: its latest artifact and how many hours old it is (null if never
    backed up). The 'are my backups current?' view."""
    store, policy, layout = env.store(), load_policy(env.policy_path()), _layout()
    now = time.time()
    rows = []
    for g in policy.db_groups:
        for a in g.names:
            arts = store.list_artifacts(layout.alias_prefix(g.id, a))
            if not arts:
                rows.append({"target": f"{g.id}/{a}", "latest": None, "age_hours": None})
                continue
            k, _s, m = max(arts, key=lambda t: t[2])
            age = round((now - m.timestamp()) / 3600, 1) if hasattr(m, "timestamp") else None
            rows.append({"target": f"{g.id}/{a}", "latest": k, "age_hours": age})
    return {"targets": rows}


def preview_restore(group: str, replace: bool = False) -> dict:
    """The blast radius of restoring a group WITHOUT mutating: per member, the artifact and what
    would happen (by-name create / drop+recreate, or alias-swap seed + swap). Runs preconditions."""
    g = load_policy(env.policy_path()).group(group)
    return {"group": group, "plan": ops.plan_restore(env.neo4j(), env.store(), g, _layout(),
                                                      replace=replace)}


def preview_prune() -> dict:
    """The artifacts retention would delete right now, WITHOUT deleting them."""
    out = ops.prune(env.store(), load_policy(env.policy_path()), _layout(), dry_run=True)
    return {"deleted": out["deleted"], "detail": out["detail"], "keys": out["keys"]}


# --- mutating (read-write scope only; each guarded by confirm) --------------------------------

def run_backup(group: str, kind: str = "AUTO") -> dict:
    """Back up every database in a group (additive — writes new artifacts). `kind` AUTO|FULL|DIFF."""
    g = load_policy(env.policy_path()).group(group)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), _layout()
    run = _run_admin(runner)
    done = [ops.backup_target(run, neo, store, runner, layout, g.id, m, kind,
                              upload=_UPLOAD, staging=_STAGING) for m in g.names]
    return {"group": group, "kind": kind, "backups": done}


def run_verify(group: str) -> dict:
    """Non-destructively check that the latest backups of a group are restorable and consistent."""
    g = load_policy(env.policy_path()).group(group)
    store, runner, layout = env.store(), env.runner(), _layout()
    run = _run_admin(runner)
    done = [ops.verify_target(run, store, runner, layout, g.id, m,
                              upload=_UPLOAD, staging=_STAGING) for m in g.names]
    return {"group": group, "verified": done}


def run_aggregate(group: str, confirm: bool = False, dry_run: bool = False) -> dict:
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
                                 upload=_UPLOAD, staging=_STAGING) for m in g.names]
    return {"group": group, "aggregated": done}


def run_restore(group: str, until: str | None = None, replace: bool = False,
                confirm: bool = False, dry_run: bool = False, verify_first: bool = True) -> dict:
    """Restore a group (alias-swap, or by-name with `replace` to DROP+recreate; `until` = PITR).
    Needs confirm=true; dry_run returns the plan. For a destructive `replace`, runs
    verify-before-drop (skip with verify_first=false)."""
    g = load_policy(env.policy_path()).group(group)
    store, neo, layout = env.store(), env.neo4j(), _layout()
    plan = ops.plan_restore(neo, store, g, layout, replace=replace)  # preconditions first
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
                            cypher_version=_CYPHER)
    return {"group": group, "verified_before_drop": verified, "restored": out}


def run_prune(confirm: bool = False, dry_run: bool = False) -> dict:
    """Delete backups past each group's retention (destructive). Needs confirm=true; dry_run (and
    the pre-confirmation result) return the exact keys that would be deleted."""
    store, policy, layout = env.store(), load_policy(env.policy_path()), _layout()
    preview = ops.prune(store, policy, layout, dry_run=True)
    if dry_run:
        return {"dry_run": True, "deleted": preview["deleted"], "detail": preview["detail"],
                "keys": preview["keys"]}
    if not confirm:
        return _needs_confirmation("prune", {"deleted": preview["deleted"], "keys": preview["keys"]})
    out = ops.prune(store, policy, layout)
    return {"deleted": out["deleted"], "detail": out["detail"]}


READ_TOOLS = [list_targets, latest_artifact, show_chain, backup_status,
              preview_restore, preview_prune]
WRITE_TOOLS = [run_backup, run_verify, run_aggregate, run_restore, run_prune]
