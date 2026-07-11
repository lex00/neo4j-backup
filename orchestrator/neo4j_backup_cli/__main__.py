"""`neo4j-backup` CLI entry point (#58 P1).

Subprocess-only adapter over `neo4j_backup_core.ops`: build the clients from the environment
(`neo4j_backup_core.env`), run each op, and emit the #60 contract — one JSON envelope on stdout
under `--json`, a documented `Exit` code, and `--dry-run` / `--confirm` / blast-radius on the
guarded (mutating) commands. Logs go to stderr so stdout stays parseable. argparse only (stdlib).

k8s execution stays with the orchestrators; here neo4j-admin runs as a local subprocess.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

from neo4j_backup_core import env, ops, paths
from neo4j_backup_core.cli_contract import Exit, envelope, make_error
from neo4j_backup_core.policy import load_policy

_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
_STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None
_CYPHER = os.environ.get("SEED_CYPHER_VERSION") or None


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def _run_admin(runner):
    """Subprocess execution of neo4j-admin (shared with the MCP server via `core.env`); its stdout
    goes to stderr so the CLI's stdout stays a clean JSON envelope (the #60 contract)."""
    return env.subprocess_admin(runner, stdout=sys.stderr)


def _group(args):
    return load_policy(env.policy_path()).group(args.group)


# --- unguarded commands (read-only or additive) -----------------------------------------------

def cmd_targets(args):
    keys = load_policy(env.policy_path()).partition_keys()
    return envelope("targets", ok=True, result={"targets": keys}), Exit.OK


def cmd_backup(args):
    group = _group(args)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), paths.get_layout()
    run = _run_admin(runner)
    done = [ops.backup_target(run, neo, store, runner, layout, group.id, name, args.kind,
                              upload=_UPLOAD, staging=_STAGING) for name in group.names]
    return envelope("backup", ok=True, group=group.id, result={"backups": done}), Exit.OK


def cmd_verify(args):
    group = _group(args)
    store, runner, layout = env.store(), env.runner(), paths.get_layout()
    run = _run_admin(runner)
    done = [ops.verify_target(run, store, runner, layout, group.id, name,
                              upload=_UPLOAD, staging=_STAGING) for name in group.names]
    return envelope("verify", ok=True, group=group.id, result={"verified": done}), Exit.OK


def cmd_system_backup(args):
    store, runner, layout = env.store(), env.runner(), paths.get_layout()
    out = ops.system_backup(_run_admin(runner), store, runner, layout,
                            upload=_UPLOAD, staging=_STAGING)
    return envelope("system-backup", ok=True, result=out), Exit.OK


def cmd_metadata_export(args):
    out = ops.export_metadata(env.neo4j(), env.store(), paths.get_layout())
    return envelope("metadata-export", ok=True, result=out), Exit.OK


# --- guarded commands (mutating: dry-run previews, --confirm to apply) -------------------------

def _guard(op, group, blast, confirmed, dry_run):
    """Shared guard: on --dry-run return the blast-radius envelope; without --confirm refuse with a
    GUARD envelope. Returns an (envelope, Exit) to short-circuit, or None to proceed with the apply."""
    if dry_run:
        return envelope(op, ok=True, group=group, result={"dry_run": True, **blast}), Exit.OK
    if not confirmed:
        return envelope(op, ok=False, group=group, result={"dry_run": False, **blast},
                        error=make_error("confirm_required",
                                         f"{op} mutates state; pass --confirm to apply or "
                                         f"--dry-run to preview")), Exit.GUARD
    return None


def cmd_restore(args):
    group = _group(args)
    store, neo, layout = env.store(), env.neo4j(), paths.get_layout()
    plan = ops.plan_restore(neo, store, group, layout, replace=args.replace)  # preconditions first
    short = _guard("restore", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.restore_group(neo, store, group, layout, restore_until=args.until,
                            replace=args.replace, cypher_version=_CYPHER, log=_log)
    return envelope("restore", ok=True, group=group.id, result=out), Exit.OK


def cmd_aggregate(args):
    group = _group(args)
    store, runner, layout = env.store(), env.runner(), paths.get_layout()
    plan = []
    for name in group.names:
        _head, physical = ops.head_physical(store, layout, group.id, name)
        plan.append({"alias": name, "physical": physical})
    short = _guard("aggregate", group.id, {"collapses": plan}, args.confirm, args.dry_run)
    if short:
        return short
    run = _run_admin(runner)
    done = [ops.aggregate_target(run, store, runner, layout, group.id, name,
                                 upload=_UPLOAD, staging=_STAGING) for name in group.names]
    return envelope("aggregate", ok=True, group=group.id, result={"aggregated": done}), Exit.OK


def cmd_prune(args):
    store, policy, layout = env.store(), load_policy(env.policy_path()), paths.get_layout()
    preview = ops.prune(store, policy, layout, dry_run=True)
    short = _guard("prune", None, {"deleted": preview["deleted"], "detail": preview["detail"],
                                   "keys": preview["keys"]}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.prune(store, policy, layout)
    return envelope("prune", ok=True, result=out), Exit.OK


def cmd_metadata_restore(args):
    store, layout = env.store(), paths.get_layout()
    key = args.key or store.latest_text_key(layout.metadata_prefix())
    if not key:
        raise ops.OpError("no metadata artifact — export first")
    short = _guard("metadata-restore", None, {"key": key}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.restore_metadata(env.neo4j(), store, layout, key)
    return envelope("metadata-restore", ok=True, result=out), Exit.OK


# --- parser + dispatch ------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="neo4j-backup",
                                description="Scheduler-agnostic Neo4j backup/restore over the "
                                            "policy-driven core. Honours the #60 CLI contract.")
    p.add_argument("--json", action="store_true", help="emit the result as one JSON object (stdout)")
    sub = p.add_subparsers(dest="command", required=True, metavar="<command>")

    def guarded(sp):
        sp.add_argument("--dry-run", action="store_true", help="preview the blast radius; mutate nothing")
        sp.add_argument("--confirm", action="store_true", help="apply the mutation (required unless --dry-run)")
        return sp

    b = sub.add_parser("backup", help="back up every database in a policy group")
    b.add_argument("group")
    b.add_argument("--kind", default="AUTO", choices=["AUTO", "FULL", "DIFF"])
    b.set_defaults(func=cmd_backup)

    v = sub.add_parser("verify", help="non-destructively check the latest backups of a group")
    v.add_argument("group")
    v.set_defaults(func=cmd_verify)

    a = guarded(sub.add_parser("aggregate", help="collapse each chain into a recovered full (in place)"))
    a.add_argument("group")
    a.set_defaults(func=cmd_aggregate)

    r = guarded(sub.add_parser("restore", help="restore a group (alias-swap or by-name)"))
    r.add_argument("group")
    r.add_argument("--until", help="ISO-8601 point-in-time (needs a differential chain)")
    r.add_argument("--replace", action="store_true", help="by-name: DROP+recreate an existing target")
    r.set_defaults(func=cmd_restore)

    pr = guarded(sub.add_parser("prune", help="delete backups past each group's retention"))
    pr.set_defaults(func=cmd_prune)

    sb = sub.add_parser("system-backup", help="binary FULL backup of the system database")
    sb.set_defaults(func=cmd_system_backup)

    t = sub.add_parser("targets", help="list the policy's group/member targets")
    t.set_defaults(func=cmd_targets)

    m = sub.add_parser("metadata", help="DBMS metadata (users/roles/privileges/aliases)")
    msub = m.add_subparsers(dest="metadata_command", required=True, metavar="<export|restore>")
    me = msub.add_parser("export", help="export the metadata layer as replayable Cypher")
    me.set_defaults(func=cmd_metadata_export)
    mr = guarded(msub.add_parser("restore", help="replay a metadata export into system"))
    mr.add_argument("--key", help="artifact key to replay (default: latest)")
    mr.set_defaults(func=cmd_metadata_restore)

    return p


def _emit(result_env: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result_env))
        return
    if result_env["ok"]:
        print(f"ok: {result_env['op']}"
              + (f" [{result_env['group']}]" if result_env.get("group") else ""))
        if result_env.get("result"):
            print(json.dumps(result_env["result"], indent=2))
    else:
        err = result_env["error"]
        print(f"error ({err['kind']}): {err['msg']}", file=sys.stderr)


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)  # argparse exits 2 (USAGE) on bad args
    try:
        result_env, code = args.func(args)
    except ops.OpError as e:
        result_env, code = envelope(args.command, ok=False,
                                    error=make_error("op_failed", str(e))), Exit.FAILURE
    except subprocess.CalledProcessError as e:
        result_env, code = envelope(args.command, ok=False,
                                    error=make_error("admin_failed",
                                                     f"neo4j-admin exited {e.returncode}")), Exit.FAILURE
    _emit(result_env, args.json)
    return int(code)


if __name__ == "__main__":
    sys.exit(main())
