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


def cmd_status(args):
    """Freshness against `rpo_minutes` (#123). `--stale` exits 1 when any target is stale."""
    from neo4j_backup_core import freshness

    policy = load_policy(env.policy_path())
    try:
        groups = (policy.select(args.group, args.tenant) if (args.group or args.tenant)
                  else policy.db_groups)
    except policy_mod.SelectionError as e:
        return envelope("status", ok=False, error=make_error("usage", str(e))), Exit.USAGE
    out = freshness.status(env.store(), paths.get_layout(), groups,
                           policy=None if (args.group or args.tenant) else policy)
    if args.stale and out["stale"]:
        return envelope("status", ok=False, result=out, error=make_error(
            "stale", f"{len(out['stale'])} target(s) past rpo_minutes: "
                     f"{', '.join(out['stale'])}")), Exit.FAILURE
    return envelope("status", ok=True, result=out), Exit.OK


def cmd_restore_points(args):
    """What a group can be restored to, per member (#113). Read-only: sidecars and load folders,
    and Neo4j only to name the current database."""
    from neo4j_backup_core import points

    policy = load_policy(env.policy_path())
    group = policy.group(args.group)
    out = points.restore_points(env.store(), paths.get_layout(), group, neo4j=env.neo4j(),
                                policy=policy)
    return envelope("restore-points", ok=True, group=group.id, result=out), Exit.OK


def cmd_backup(args):
    group = _group(args)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), paths.get_layout()
    run = _run_admin(runner)
    done = [ops.backup_target(run, neo, store, runner, layout, group.id, name, args.kind,
                              upload=_UPLOAD, staging=_STAGING, log=_log, group=group)
            for name in group.names]
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
    if args.to_dump:  # import, dump offline, store under the member's _seed/ (#158)
        group, _, member = args.to_dump.partition("/")
        pol = load_policy(env.policy_path())
        if group not in {g.id for g in pol.db_groups} or member not in pol.group(group).names:
            return envelope("import", ok=False, error=make_error(
                "usage", f"--to-dump {args.to_dump!r}: not a <group>/<member> in the policy")), \
                Exit.USAGE
        out = ops.import_to_dump(_run_admin(runner), runner, env.store(), paths.get_layout(),
                                 group, member, args.database, source,
                                 upload=_UPLOAD, staging=_STAGING, log=_log)
        return envelope("import", ok=True, group=group, result=out), Exit.OK
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


def cmd_load(args):
    """Load or rebuild a group (#107): build in the runner, dump, seed, check, set access, swap,
    baseline. `--reapply-access` only applies the policy's `writes` to the current databases."""
    from neo4j_backup_core import load as load_mod

    group = _group(args)
    store, runner, neo, layout = env.store(), env.runner(), env.neo4j(), paths.get_layout()
    if args.reapply_access:
        blast = {"reapply_access": [{"member": n, "writes": group.load.writes}
                                    for n in group.names]}
        short = _guard("load", group.id, blast, args.confirm, args.dry_run)
        if short:
            return short
        out = load_mod.reapply_access(neo, group, log=_log)
        return envelope("load", ok=True, group=group.id, result={"access": out}), Exit.OK
    plan = load_mod.plan_load(neo, store, layout, group, replace=args.replace)
    short = _guard("load", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    try:
        out = load_mod.load_group(_run_admin(runner), runner, neo, store, layout, group,
                                  replace=args.replace, keep_failed=args.keep_failed,
                                  baseline=False if args.no_baseline else None,
                                  upload=_UPLOAD, staging=_STAGING, cypher_version=_CYPHER,
                                  log=_log)
    except load_mod.LoadError as e:
        return envelope("load", ok=False, group=group.id,
                        result={**e.result, "failed_step": e.step},
                        error=make_error("load_failed", str(e))), Exit.FAILURE
    except ops.BaselineError as e:
        return envelope("load", ok=False, group=group.id, result={"baseline": e.done},
                        error=make_error(e.kind, str(e))), Exit.FAILURE
    return envelope("load", ok=True, group=group.id, result=out), Exit.OK


def cmd_adopt(args):
    """Bring existing databases under the alias model (#92)."""
    from neo4j_backup_core import adopt

    group = _group(args)
    neo = env.neo4j()
    plan = adopt.plan_adopt(neo, group, migrate=args.migrate, replace=args.replace)
    short = _guard("adopt", group.id, {"plan": plan, "drops": plan["drops"]}, args.confirm,
                   args.dry_run)
    if short:
        return short
    runner = env.runner()
    out = adopt.adopt(_run_admin(runner), neo, env.store(), runner, paths.get_layout(), group,
                      migrate=args.migrate, replace=args.replace, upload=_UPLOAD,
                      staging=_STAGING, log=_log)
    return envelope("adopt", ok=True, group=group.id, result=out), Exit.OK


def cmd_cleanup(args):
    """Drop superseded physicals of a group (#93), keeping the current one and `--keep` before
    it; databases the tool didn't create are never touched."""
    group = _group(args)
    neo = env.neo4j()
    plan = ops.plan_cleanup(neo, group, keep=args.keep, older_than_days=args.older_than)
    short = _guard("cleanup", group.id, {"plan": plan, "drops": plan["drops"]}, args.confirm,
                   args.dry_run)
    if short:
        return short
    out = ops.cleanup(neo, group, keep=args.keep, older_than_days=args.older_than, log=_log)
    return envelope("cleanup", ok=True, group=group.id, result=out), Exit.OK


def cmd_rollback(args):
    """Point each alias back at its previous database (#122)."""
    group = _group(args)
    store, neo = env.store(), env.neo4j()
    plan = ops.rollback(neo, store, group, members=args.member, dry_run=True)
    short = _guard("rollback", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.rollback(neo, store, group, members=args.member, log=_log)
    return envelope("rollback", ok=True, group=group.id, result=out), Exit.OK


def cmd_export(args):
    """A portable dump per member, at --until or the latest backup (#160). Writes to `_seed/`;
    never touches the cluster. Guarded like the other writes that act on a whole group."""
    group = _group(args)
    store, runner, layout = env.store(), env.runner(), paths.get_layout()
    seeds = ops.choose_seeds(store, layout, group, args.until)  # fails before any work
    plan = {"members": [{"member": n, "from": s["key"], "until": s["until"],
                         "chain": s.get("chain"), "already_a_dump": bool(s.get("load"))}
                        for n, s in seeds.items()]}
    short = _guard("export", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.export_dumps(_run_admin(runner), runner, store, layout, group, until=args.until,
                           log=_log)
    return envelope("export", ok=True, group=group.id, result=out), Exit.OK


def _loads_arg(args):
    """--from-load (every member's newest load) or --from-dump MEMBER=KEY_OR_URI (#144, #159)."""
    if args.from_dump:
        out = {}
        for item in args.from_dump:
            member, sep, key = item.partition("=")
            if not sep or not key:
                raise ops.OpError(f"--from-dump {item!r}: expected MEMBER=KEY_OR_URI")
            out[member] = key
        return out
    return "latest" if args.from_load else None


def cmd_move(args):
    """`restore --to-cluster` (#115): one group, or every group of `--from-cluster`."""
    from neo4j_backup_core import move

    bad = [f for f, v in (("--member", args.member), ("--from-load", args.from_load),
                          ("--from-dump", args.from_dump), ("--replace", args.replace),
                          ("--check", args.check)) if v]
    if bad:
        return envelope("restore", ok=False, group=args.group, error=make_error(
            "usage", f"--to-cluster doesn't take {', '.join(bad)}")), Exit.USAGE
    pol = load_policy(env.policy_path())
    groups, source = move.groups_for(pol, args.group, args.from_cluster)
    store, neo, layout = env.store(), env.neo4j(), paths.get_layout()
    kw = dict(until=args.until, fence_source=args.fence_source,
              skip_metadata=args.no_metadata, clusters=pol.cluster_names())
    plan = move.public(move.plan_move(neo, store, layout, groups, source, args.to_cluster, **kw))
    plan["baseline"] = not args.no_baseline
    short = _guard("restore", args.group, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = move.public(move.move(neo, store, layout, groups, source, args.to_cluster,
                                cypher_version=_CYPHER, log=_log, **kw))
    if not args.no_baseline:
        runner = env.runner()
        try:
            out["baseline"] = move.baseline(_run_admin(runner), neo, store, runner, layout,
                                            groups, out, upload=_UPLOAD, staging=_STAGING,
                                            log=_log)
        except ops.BaselineError as e:
            return envelope("restore", ok=False, group=args.group,
                            result={**out, "baseline": e.done},
                            error=make_error(ops.BaselineError.kind, str(e))), Exit.FAILURE
    return envelope("restore", ok=True, group=args.group, result=out), Exit.OK


def cmd_restore(args):
    if args.to_cluster or args.from_cluster:
        if not args.to_cluster:
            return envelope("restore", ok=False, error=make_error(
                "usage", "--from-cluster needs --to-cluster")), Exit.USAGE
        return cmd_move(args)
    if not args.group:
        return envelope("restore", ok=False, error=make_error(
            "usage", "name a group to restore")), Exit.USAGE
    group = ops.only_members(_group(args), args.member)  # some members only (#121)
    store, neo, layout = env.store(), env.neo4j(), paths.get_layout()
    loads = _loads_arg(args)
    plan = ops.plan_restore(neo, store, group, layout, replace=args.replace, until=args.until,
                            loads=loads)  # preconditions first, incl. the chain for --until
    plan["baseline"] = not args.no_baseline  # a FULL of each new database afterwards (#97)
    short = _guard("restore", group.id, {"plan": plan}, args.confirm, args.dry_run)
    if short:
        return short
    out = ops.restore_group(neo, store, group, layout, restore_until=args.until,
                            replace=args.replace, cypher_version=_CYPHER, log=_log, loads=loads,
                            check=args.check, keep_failed=args.keep_failed)
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
                                                           "kept_for_chains", "unknown_kept", "loads")},
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
                                            "policy-driven core. See CLI-CONTRACT.md for output and exit codes.")
    p.add_argument("--json", action="store_true", help="emit the result as one JSON object (stdout)")
    p.add_argument("--debug", action="store_true",
                   help="print the traceback of an unexpected error to stderr (or set "
                        "NEO4J_BACKUP_DEBUG=1)")
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

    rp = selectable(sub.add_parser("restore-points",
                                   help="what each member can be restored to (read-only)"))
    rp.set_defaults(func=_by_tenant(cmd_restore_points, "restore-points"))

    v = selectable(sub.add_parser("verify", help="non-destructively check the latest backups of a group"))
    v.set_defaults(func=_by_tenant(cmd_verify, "verify"))

    a = guarded(selectable(sub.add_parser(
        "aggregate", help="collapse each chain into a recovered full (in place)")))
    a.set_defaults(func=_recorded(_by_tenant(cmd_aggregate, "aggregate")))

    r = guarded(sub.add_parser("restore", help="restore a group (alias-swap or by-name)"))
    r.add_argument("group", nargs="?")
    r.add_argument("--to-cluster", metavar="ID",
                   help="restore into another cluster of the policy")
    r.add_argument("--from-cluster", metavar="ID",
                   help="with --to-cluster: every group of this cluster in one plan")
    r.add_argument("--fence-source", action="store_true",
                   help="with --to-cluster: the source still answers; stop the groups' backups "
                        "and loads there")
    r.add_argument("--no-metadata", action="store_true",
                   help="with --to-cluster: don't replay the source's users and roles")
    r.add_argument("--until", type=_until_arg,
                   help="ISO-8601 point-in-time, UTC if no offset (needs a differential chain)")
    r.add_argument("--replace", action="store_true", help="by-name: DROP+recreate an existing target")
    src = r.add_mutually_exclusive_group()
    src.add_argument("--from-load", action="store_true",
                     help="seed every member from its newest load artifact (_seed/)")
    src.add_argument("--from-dump", action="append", metavar="MEMBER=KEY_OR_URI",
                     help="seed a member from a dump: a key in its _seed/ folder, or a URI the "
                          "servers can read (an external dump); repeat per member")
    r.add_argument("--member", action="append", metavar="ALIAS_OR_DATABASE",
                   help="restore only this member (repeatable); the rest of the group stays")
    r.add_argument("--check", action="store_true",
                   help="run the group's load.checks on the new databases before the swap")
    r.add_argument("--keep-failed", action="store_true",
                   help="with --check: keep the new databases when a check fails")
    r.add_argument("--no-baseline", action="store_true",
                   help="skip the full backup of each restored database (taken by default)")
    r.set_defaults(func=_recorded(cmd_restore))

    ld = guarded(sub.add_parser("load", help="load or rebuild a group from its policy build "
                                             ""))
    ld.add_argument("group")
    ld.add_argument("--replace", action="store_true", help="by-name: DROP+recreate existing")
    ld.add_argument("--keep-failed", action="store_true",
                    help="keep the new databases when a step before the swap fails")
    ld.add_argument("--no-baseline", action="store_true", help="skip the baseline backup")
    ld.add_argument("--reapply-access", action="store_true",
                    help="only apply the policy's `writes` to the current databases")
    ld.set_defaults(func=_recorded(cmd_load))

    ad = guarded(sub.add_parser("adopt", help="bring existing databases under the alias model "
                                              ""))
    ad.add_argument("group")
    ad.add_argument("--migrate", action="store_true",
                    help="same-name members: back up, seed a new physical, DROP the original, "
                         "create the alias (destructive; needs --replace)")
    ad.add_argument("--replace", action="store_true", help="allow the DROP in --migrate")
    ad.set_defaults(func=_recorded(cmd_adopt))

    cu = guarded(sub.add_parser("cleanup", help="drop superseded databases of a group"))
    cu.add_argument("group")
    cu.add_argument("--keep", type=int, default=1,
                    help="previous databases kept for rollback (default 1)")
    cu.add_argument("--older-than", type=float, metavar="DAYS",
                    help="only databases created more than DAYS ago")
    cu.set_defaults(func=_recorded(cmd_cleanup))

    rb = guarded(sub.add_parser("rollback", help="point each alias back at the database it "
                                                 "targeted before the last swap"))
    rb.add_argument("group")
    rb.add_argument("--member", action="append", metavar="ALIAS",
                    help="only this member (repeatable)")
    rb.set_defaults(func=_recorded(cmd_rollback))

    ex = guarded(sub.add_parser("export", help="a portable .dump of each member, at --until or "
                                               "the latest backup"))
    ex.add_argument("group")
    ex.add_argument("--until", type=_until_arg, help="ISO-8601 point in time, UTC if no offset")
    ex.set_defaults(func=_recorded(cmd_export))

    pr = guarded(sub.add_parser("prune", help="delete backups past each group's retention"))
    pr.set_defaults(func=_recorded(cmd_prune))

    def clustered(sp):  # DBMS-wide commands act on one cluster (#106)
        sp.add_argument("--cluster", default="default", help="the policy cluster (default: default)")
        return sp

    sb = clustered(sub.add_parser("system-backup", help="binary FULL backup of the system database"))
    sb.set_defaults(func=cmd_system_backup)

    t = selectable(sub.add_parser("targets", help="list the policy's group/member targets"))
    t.set_defaults(func=cmd_targets)

    stt = selectable(sub.add_parser("status", help="freshness of each target against its "
                                                   "rpo_minutes"))
    stt.add_argument("--stale", action="store_true",
                     help="exit 1 when any target is stale (for cron or CI alerts)")
    stt.set_defaults(func=cmd_status)

    im = sub.add_parser("import", help="bulk-import raw data into an offline store on the loader")
    im.add_argument("database", help="the physical database name to import into (see IMPORT.md)")
    im.add_argument("--to-dump", metavar="GROUP/MEMBER",
                    help="then dump the offline store into that member's load folder (_seed/)")
    im.add_argument("source_args", nargs=argparse.REMAINDER,
                    help="passthrough to neo4j-admin (e.g. --nodes=/path.csv --relationships=/rel.csv)")
    im.set_defaults(func=cmd_import)

    po = sub.add_parser("policy", help="inspect or check the policy (no cluster or store access)")
    posub = po.add_subparsers(dest="policy_command", required=True, metavar="<show|validate|check>")
    ps = posub.add_parser("show", help="a group's effective settings and where each came from")
    ps.add_argument("group")
    ps.set_defaults(func=cmd_policy_show)
    pc = posub.add_parser("check", help="can the tool list and write each store?")
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
        policy_mod.check_upload_mode()  # #218
        result_env, code = args.func(args)
    except ops.OpError as e:
        extra = ({"result": {"dropped": e.dropped, "kept": e.kept}}
                 if isinstance(e, ops.SeedError) else
                 {"result": {"moved": e.moved, "not_moved": e.not_moved}}
                 if isinstance(e, ops.CutoverError) else {})
        result_env, code = envelope(args.command, ok=False, **extra,
                                    error=make_error(getattr(e, "kind", "op_failed"),
                                                     str(e))), Exit.FAILURE
    except subprocess.CalledProcessError as e:
        result_env, code = envelope(args.command, ok=False,
                                    error=make_error("admin_failed",
                                                     f"neo4j-admin exited {e.returncode}")), Exit.FAILURE
    except Exception as e:  # noqa: BLE001 - every failure keeps the contract: one envelope (#140)
        if args.debug or os.environ.get("NEO4J_BACKUP_DEBUG"):
            import traceback
            traceback.print_exc(file=sys.stderr)
        kind = ("neo4j_error" if type(e).__module__.split(".")[0] == "neo4j"
                else "internal_error")
        result_env, code = envelope(args.command, ok=False, error=make_error(
            kind, f"{type(e).__name__}: {e}")), Exit.FAILURE
    _emit(result_env, args.json)
    return int(code)


if __name__ == "__main__":
    sys.exit(main())
