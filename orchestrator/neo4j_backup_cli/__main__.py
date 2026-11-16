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

from neo4j_backup_core import env, ops, paths, record
from neo4j_backup_core.cli_contract import Exit, envelope, make_error
from neo4j_backup_core import policy as policy_mod
from neo4j_backup_core.policy import load_policy
from neo4j_backup_core.until import UntilError, parse_until

_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
_STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None
_CYPHER = os.environ.get("SEED_CYPHER_VERSION") or None


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def _run_admin(runner):
    """Subprocess execution of neo4j-admin (shared with the MCP server via `core.env`); its stdout
    goes to stderr so the CLI's stdout stays a clean JSON envelope (the #60 contract)."""
    return env.subprocess_admin(runner, stdout=sys.stderr)


def _until_arg(value: str) -> str:
    """argparse type for --until (#77): a bad value is a usage error (exit 2) before any call."""
    try:
        return parse_until(value)
    except UntilError as e:
        raise argparse.ArgumentTypeError(str(e)) from None


def _group(args):
    return load_policy(env.policy_path()).group(args.group)


# --- unguarded commands (read-only or additive) -----------------------------------------------

def cmd_targets(args):
    policy = load_policy(env.policy_path())
    groups = policy.db_groups
    if args.group or args.tenant:  # optional filters (#102)
        try:
            groups = policy.select(args.group, args.tenant)
        except policy_mod.SelectionError as e:
            return envelope("targets", ok=False, error=make_error("usage", str(e))), Exit.USAGE
    keys = [f"{g.id}/{n}" for g in groups for n in g.names]
    return envelope("targets", ok=True, result={
        "targets": keys, "tenants": {g.id: g.tenant for g in groups},
        "clusters": {g.id: getattr(g, "cluster", None) or "default" for g in groups},
        "warnings": policy.warnings()}), Exit.OK


def _by_tenant(cmd, op):
    """Let a per-group command take `--tenant` instead of a group (#102): run it for every group
    with that tenant label and collect the per-group envelopes under `result.groups`. A group
    keeps today's single-group result. Both, neither, or an unknown name is a usage error."""
    def run(args):
        try:
            groups = load_policy(env.policy_path()).select(args.group, args.tenant)
        except policy_mod.SelectionError as e:
            return envelope(op, ok=False, group=args.group,
                            error=make_error("usage", str(e))), Exit.USAGE
        if not args.tenant:
            return cmd(args)
        envs, code = [], Exit.OK
        for g in groups:
            args.group = g.id
            e, c = cmd(args)
            envs.append({"group": g.id, "ok": e["ok"], "result": e.get("result"),
                         "error": e.get("error")})
            code = max(code, c)
        args.group = None
        ok = all(e["ok"] for e in envs)
        err = None if ok else make_error(
            next(e["error"]["kind"] for e in envs if not e["ok"]),
            "; ".join(f"{e['group']}: {e['error']['msg']}" for e in envs if not e["ok"]))
        return envelope(op, ok=ok, result={"tenant": args.tenant, "groups": envs},
                        error=err), code
    return run


def cmd_backup(args):
    group = _group(args)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), paths.get_layout()
    run = _run_admin(runner)
    done = [ops.backup_target(run, neo, store, runner, layout, group.id, name, args.kind,
                              upload=_UPLOAD, staging=_STAGING, log=_log) for name in group.names]
    # a DIFF with no full to diff against is skipped, not failed (#74); the caller runs a FULL
    needs = [d["alias"] for d in done if d.get("needs_full")]
    return envelope("backup", ok=True, group=group.id,
                    result={"backups": done, "needs_full": needs}), Exit.OK


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
                            upload=_UPLOAD, staging=_STAGING, cluster=args.cluster)
    return envelope("system-backup", ok=True, result=out), Exit.OK


def cmd_metadata_export(args):
    out = ops.export_metadata(env.neo4j(), env.store(), paths.get_layout(), cluster=args.cluster)
    return envelope("metadata-export", ok=True, result=out), Exit.OK


def cmd_policy_show(args):
    """The group as every front-end sees it, after profiles (#101), with each value's source."""
    try:
        out = policy_mod.explain(env.policy_path(), args.group)
    except KeyError:
        return envelope("policy-show", ok=False, group=args.group,
                        error=make_error("usage", f"no group {args.group!r} in the policy")), \
            Exit.USAGE
    if not args.json:
        for k, v in out["group"].items():
            if k == "load":
                for kk, vv in v.items():
                    print(f"load.{kk}: {vv}  [{out['from'][f'load.{kk}']}]", file=sys.stderr)
            else:
                print(f"{k}: {v}  [{out['from'][k]}]", file=sys.stderr)
    return envelope("policy-show", ok=True, group=args.group, result=out), Exit.OK


def cmd_policy_check(args):
    """List, write and delete a probe in every store the policy uses; say which stores the Neo4j
    servers must be able to read. Exit 1 if any store fails."""
    policy = load_policy(env.policy_path())
    rows = env.check_stores(policy, env.store())
    for r in rows:
        state = "ok" if r["list"] and r["write"] else "FAILED " + "; ".join(r["errors"])
        _log(f"{r['store']} {r['url']}: {state}")
    bad = [r["store"] for r in rows if not (r["list"] and r["write"])]
    if bad:
        return envelope("policy-check", ok=False, result={"stores": rows},
                        error=make_error("store_unreachable",
                                         f"can't list or write: {', '.join(bad)}")), Exit.FAILURE
    return envelope("policy-check", ok=True, result={"stores": rows}), Exit.OK


def cmd_policy_validate(args):
    """Validate a policy file without touching any cluster or store: exit 0 valid, 2 invalid."""
    errors = policy_mod.validate_file(args.file)
    if errors:
        for e in errors:
            print(f"invalid: {e}", file=sys.stderr)
        return envelope("policy-validate", ok=False, result={"file": args.file, "errors": errors},
                        error=make_error("invalid_policy", "; ".join(errors))), Exit.USAGE
    return envelope("policy-validate", ok=True, result={"file": args.file, "errors": []}), Exit.OK


def cmd_import(args):
    # Bulk import raw data into an offline store on the loader (#16). source_args is a raw passthrough
    # to neo4j-admin (a leading `--` separator, if the operator adds one, is dropped). See IMPORT.md.
    source = args.source_args[1:] if args.source_args[:1] == ["--"] else args.source_args
    runner = env.runner()
    out = ops.import_database(_run_admin(runner), runner, args.database, source)
    return envelope("import", ok=True, result=out), Exit.OK


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
    plan = ops.plan_restore(neo, store, group, layout, replace=args.replace,
                            until=args.until)  # preconditions first, incl. the chain for --until
    plan["baseline"] = not args.no_baseline  # a FULL of each new database afterwards (#97)
    short = _guard("restore", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.restore_group(neo, store, group, layout, restore_until=args.until,
                            replace=args.replace, cypher_version=_CYPHER, log=_log)
    if args.no_baseline:
        return envelope("restore", ok=True, group=group.id,
                        result={**out, "baseline": None}), Exit.OK
    runner = env.runner()
    try:
        out["baseline"] = ops.baseline_after_restore(
            _run_admin(runner), neo, store, runner, layout, group, out["members"],
            upload=_UPLOAD, staging=_STAGING, log=_log)
    except ops.BaselineError as e:
        # the restore is applied; say so, and what's left to back up
        return envelope("restore", ok=False, group=group.id,
                        result={**out, "baseline": e.done},
                        error=make_error(ops.BaselineError.kind, str(e))), Exit.FAILURE
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
                                 upload=_UPLOAD, staging=_STAGING, log=_log)
            for name in group.names]
    return envelope("aggregate", ok=True, group=group.id, result={"aggregated": done}), Exit.OK


def cmd_prune(args):
    store, policy, layout = env.store(), load_policy(env.policy_path()), paths.get_layout()
    runner = env.runner()
    # backups taken before #118 have no sidecar; inspecting them (additive) lets prune see chains
    fill = {"run_admin": _run_admin(runner), "runner": runner, "log": _log}
    preview = ops.prune(store, policy, layout, dry_run=True, **fill)
    short = _guard("prune", None, {k: preview[k] for k in ("deleted", "detail", "keys",
                                                           "kept_for_chains", "unknown_kept")},
                   args.confirm, args.dry_run)
    if short:
        return short
    out = ops.prune(store, policy, layout, **fill)
    return envelope("prune", ok=True, result=out), Exit.OK


def cmd_metadata_restore(args):
    store, layout = env.store(), paths.get_layout()
    key = args.key or ops._cluster(store, args.cluster).latest_text_key(
        layout.metadata_listing(args.cluster))
    if not key:
        raise ops.OpError("no metadata artifact — export first")
    short = _guard("metadata-restore", None, {"key": key}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.restore_metadata(env.neo4j(), store, layout, key, cluster=args.cluster)
    return envelope("metadata-restore", ok=True, result=out), Exit.OK


def _recorded(cmd):
    """Wrap a guarded (mutating) command: check `--ticket` / `--reason`, log them, and return
    them in the result (#119). A missing ticket when one is required refuses like the confirm
    guard (exit 3) before anything runs; a dry run never needs one."""
    def run(args):
        try:
            rec = record.fields(args.ticket, args.reason,
                                applying=args.confirm and not args.dry_run)
        except record.TicketError as e:
            return envelope(args.command, ok=False, error=make_error(e.kind, str(e))), Exit.GUARD
        _log(record.describe(args.command, rec))
        try:
            env_, code = cmd(args)
        except ops.OpError as e:
            env_, code = envelope(args.command, ok=False,
                                  error=make_error("op_failed", str(e))), Exit.FAILURE
        env_["result"] = {**(env_.get("result") or {}), **rec}
        return env_, code
    return run


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
        sp.add_argument("--ticket", help="the change ticket this runs under; recorded with the result")
        sp.add_argument("--reason", help="why it runs; recorded with the result")
        return sp

    def selectable(sp):  # a group, or --tenant for all of a tenant's groups (#102)
        sp.add_argument("group", nargs="?", help="the policy group (or use --tenant)")
        sp.add_argument("--tenant", help="every group with this tenant label, instead of a group")
        return sp

    b = selectable(sub.add_parser("backup", help="back up every database in a policy group"))
    b.add_argument("--kind", default="AUTO", choices=["AUTO", "FULL", "DIFF"])
    b.set_defaults(func=_by_tenant(cmd_backup, "backup"))

    v = selectable(sub.add_parser("verify", help="non-destructively check the latest backups of a group"))
    v.set_defaults(func=_by_tenant(cmd_verify, "verify"))

    a = guarded(selectable(sub.add_parser(
        "aggregate", help="collapse each chain into a recovered full (in place)")))
    a.set_defaults(func=_recorded(_by_tenant(cmd_aggregate, "aggregate")))

    r = guarded(sub.add_parser("restore", help="restore a group (alias-swap or by-name)"))
    r.add_argument("group")
    r.add_argument("--until", type=_until_arg,
                   help="ISO-8601 point-in-time, UTC if no offset (needs a differential chain)")
    r.add_argument("--replace", action="store_true", help="by-name: DROP+recreate an existing target")
    r.add_argument("--no-baseline", action="store_true",
                   help="skip the full backup of each restored database (taken by default, #97)")
    r.set_defaults(func=_recorded(cmd_restore))

    pr = guarded(sub.add_parser("prune", help="delete backups past each group's retention"))
    pr.set_defaults(func=_recorded(cmd_prune))

    def clustered(sp):  # DBMS-wide commands act on one cluster (#106)
        sp.add_argument("--cluster", default="default", help="the policy cluster (default: default)")
        return sp

    sb = clustered(sub.add_parser("system-backup", help="binary FULL backup of the system database"))
    sb.set_defaults(func=cmd_system_backup)

    t = selectable(sub.add_parser("targets", help="list the policy's group/member targets"))
    t.set_defaults(func=cmd_targets)

    im = sub.add_parser("import", help="bulk-import raw data into an offline store on the loader (#16)")
    im.add_argument("database", help="the physical database name to import into (see IMPORT.md)")
    im.add_argument("source_args", nargs=argparse.REMAINDER,
                    help="passthrough to neo4j-admin (e.g. --nodes=/path.csv --relationships=/rel.csv)")
    im.set_defaults(func=cmd_import)

    po = sub.add_parser("policy", help="inspect or check the policy (no cluster or store access)")
    posub = po.add_subparsers(dest="policy_command", required=True, metavar="<show|validate>")
    ps = posub.add_parser("show", help="a group's effective settings and where each came from")
    ps.add_argument("group")
    ps.set_defaults(func=cmd_policy_show)
    pc = posub.add_parser("check", help="can the tool list and write each store? (#105)")
    pc.set_defaults(func=cmd_policy_check)
    pv = posub.add_parser("validate", help="load and validate a policy file; exit 0 or 2")
    pv.add_argument("file")
    pv.set_defaults(func=cmd_policy_validate)

    m = sub.add_parser("metadata", help="DBMS metadata (users/roles/privileges/aliases)")
    msub = m.add_subparsers(dest="metadata_command", required=True, metavar="<export|restore>")
    me = clustered(msub.add_parser("export", help="export the metadata layer as replayable Cypher"))
    me.set_defaults(func=cmd_metadata_export)
    mr = clustered(guarded(msub.add_parser("restore", help="replay a metadata export into system")))
    mr.add_argument("--key", help="artifact key to replay (default: latest)")
    mr.set_defaults(func=_recorded(cmd_metadata_restore))

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
