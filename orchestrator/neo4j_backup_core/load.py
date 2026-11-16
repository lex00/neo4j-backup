"""Loads and rebuilds run by the tool (#107): build the group's data in the runner, dump it,
seed a new physical from the dump, check it, set its access, swap the alias, take the baseline.

One member at a time is built and seeded; the swap happens for every member together after all
of them passed their checks, so a group never ends up half loaded. Any failure before the swap
drops the new physicals and leaves every alias where it was; the result names the failed step.

Sources: a `kind: import` build's relative input paths (`--nodes=Product=products/*.parquet`)
resolve against the group's `load.source` (default: the group's store, `<group>/_source/`, and
`<slug>/` below it when the group has more than one member); neo4j-admin import reads them from
the store directly. Absolute paths and URIs are passed through.
"""

from __future__ import annotations

import re

from . import checks, naming
from .ops import (OpError, _NOLOG, _admin, _per, _privileges_of, _store, _upload, apply_access,
                  backs_up, baseline_after_restore, check_seeded, cutover_seeded, _topology)

_PATH_FLAGS = ("--nodes", "--relationships")


class LoadError(OpError):
    """A load stopped before the swap. `step` names where; `result` has what was done."""

    def __init__(self, msg: str, step: str, result: dict):
        super().__init__(msg)
        self.step, self.result = step, result


def source_prefix(group, member: str, layout=None) -> str:
    src = getattr(group.load, "source", None)
    base = (src.prefix if src and src.prefix else f"{group.id}/_source/")
    base = base if base.endswith("/") else base + "/"
    return base if len(group.names) == 1 else f"{base}{naming.slug(member)}/"


def resolve_args(args: list[str], uri_of) -> list[str]:
    """`--nodes=[Label=]path[,path…]` and `--relationships=…`: each relative path becomes
    `uri_of(path)`; absolute paths, URIs and other options are left alone."""
    out = []
    for a in args:
        flag, sep, rest = a.partition("=")
        if not sep or not flag.startswith(_PATH_FLAGS):
            out.append(a)
            continue
        label, lsep, paths = rest.rpartition("=")
        fixed = ",".join(p if (p.startswith("/") or re.match(r"^[a-z0-9]+:", p)) else uri_of(p)
                         for p in paths.split(","))
        out.append(f"{flag}={label}{lsep}{fixed}")
    return out


def plan_load(neo4j, store, layout, group, *, replace: bool = False) -> dict:
    """What `load` would do, without doing it: per member the source objects, the build argv,
    the target, the alias move, and what is lost (#107)."""
    load = group.load
    if load.mode == "live":
        raise OpError(f"group {group.id!r} has load.mode 'live': nothing to load")
    if load.build is None:
        raise OpError(f"group {group.id!r} has no load.build; load it by hand (IMPORT.md) and "
                      f"restore with --from-dump")
    src_store = _source_store(store, group)
    members = []
    for name in group.names:
        sp = source_prefix(group, name)
        objects = [k for (k, _m) in src_store.list_text_keys(sp, "")] \
            if hasattr(src_store, "list_text_keys") else []
        neo = _per(neo4j, group.id)
        if load.build.kind == "command":
            build = {"kind": "command", "command": load.build.command, "image": load.build.image,
                     "args": load.build.args, "output": load.build.output,
                     "import_args": load.build.import_args, "timeout": load.build.timeout,
                     "env": sorted(load.build.env)}  # names only: values may be secrets
        else:
            build = ["neo4j-admin", "database", "import", "full", "<build db>",
                     *resolve_args(load.build.args, lambda p, sp=sp: src_store.uri(sp + p))]
        entry = {"member": name, "source": src_store.uri(sp), "objects": objects,
                 "build": build, "writes": load.writes}
        if group.restore_mode == "by-name":
            existed = neo.database_exists(name)
            if existed and not replace:
                raise OpError(f"database {name!r} exists; set replace=true to DROP+recreate it")
            entry.update(target=name, action="drop+recreate" if existed else "create",
                         drops=[name] if existed else [])
        else:
            entry.update(old=neo.alias_target(name), action="seed new physical, swap alias",
                         drops=[])
        entry["loses"] = ("writes since the previous load" if load.writes == "read-write"
                          else "nothing (read-only)")
        members.append(entry)
    return {"group": group.id, "mode": load.mode, "members": members,
            "drops": [d for m in members for d in m["drops"]]}


def _source_store(store, group):
    src = getattr(group.load, "source", None)
    if src and src.storage and hasattr(store, "named"):
        return store.named(src.storage)
    return _store(store, group.id)


def load_group(run_admin, runner, neo4j, store, layout, group, *, replace: bool = False,
               keep_failed: bool = False, baseline: bool | None = None, upload: str = "admin",
               staging: str | None = None, cypher_version: str | None = None,
               log=_NOLOG, sleep=None) -> dict:
    """Build, dump, seed, check, set access, swap, baseline (#107). See the module docstring."""
    from .ops import check_fence
    check_fence(neo4j, store, group.id)  # moved to another cluster (#115)
    plan = plan_load(neo4j, store, layout, group, replace=replace)
    load = group.load
    result_notice = notify(group, plan, log=log, sleep=sleep)
    neo4j, runner = _per(neo4j, group.id), _per(runner, group.id)
    store, src_store = _store(store, group.id), _source_store(store, group)
    upload = _upload(runner, upload)
    cypher_version = getattr(neo4j, "cypher_version", None) or cypher_version
    ts = naming.ts_for(group)  # the group's naming.ts_format (#88)
    result = {"group": group.id, "mode": load.mode, "members": [], "notice": result_notice}
    seeded: list[dict] = []
    step = "build"
    try:
        for name in group.names:
            step = f"build {name}"
            sp = source_prefix(group, name)
            local = f"load-{naming.slug(name)}-{ts}"[:63]
            prefix = f"{layout.load_prefix(group.id, name)}{ts}/"
            if load.build.kind == "command":
                out = _command_build(run_admin, runner, store, src_store, group, name, local,
                                     sp, prefix, ts)
            else:
                args = resolve_args(load.build.args, lambda p, sp=sp: src_store.uri(sp + p))
                if load.build.input_type == "parquet":
                    args = ["--input-type=parquet", *args]
                out = _admin(run_admin,
                             runner.import_dump_command(local, args, store.uri(prefix)), store)
            key = f"{prefix}{local}.dump"
            counts = checks.import_counts(out or "")
            log(f"{group.id}/{name}: built and dumped -> {key} ({counts})")
            step = f"seed {name}"
            entry = {"mode": group.restore_mode, "group": group.id, "dump": key,
                     "key": key, "import_counts": counts}
            if group.restore_mode == "by-name":
                entry.update(name=name, replaced=neo4j.database_exists(name))
                seeded.append(entry)  # by-name is seeded at the swap (DROP + CREATE)
                continue
            old = neo4j.alias_target(name)
            newdb = naming.for_group(group).physical(name, ts, old)  # (#88, #89)
            if newdb == old or neo4j.database_exists(newdb):
                raise OpError(f"{group.id}/{name}: the new physical name {newdb!r} is taken")
            entry.update(alias=name, newdb=newdb, old=old)
            neo4j.seed_database(newdb, store.uri(key), topology=_topology(neo4j, group, name),
                                cypher_version=cypher_version, store_base=store.uri(""))
            seeded.append(entry)
            step = f"access {name}"
            entry["access"] = apply_access(neo4j, group, newdb, log=log)
        step = "checks"
        swaps = [s for s in seeded if s["mode"] != "by-name"]
        check_seeded(neo4j, group, swaps, keep_failed=True, log=log,
                     expect_counts={s["alias"]: s["import_counts"] for s in swaps
                                    if s.get("import_counts")})
    except Exception as e:  # noqa: BLE001 - name the step, drop what was seeded, re-raise
        if not keep_failed:
            for s in seeded:
                if s.get("newdb"):
                    neo4j.drop_database(s["newdb"])
        result["members"] = seeded
        raise LoadError(f"load of {group.id} stopped at {step}: {e}; no alias was moved"
                        + ("" if keep_failed else "; new databases dropped"), step, result) from e

    step = "swap"
    for s in seeded:
        if s["mode"] == "by-name":  # DROP + CREATE from the dump, privileges kept (#117)
            from .ops import _restore_by_name
            out = _restore_by_name(neo4j, group, s["name"], s["key"], s["replaced"],
                                   store=store, restore_until=None,
                                   cypher_version=cypher_version, log=log)
            s.update(privileges=out["privileges"])
            s["checks"] = checks.run(neo4j, s["name"], list(load.checks),
                                     expect_counts=s.get("import_counts"))
    # privileges first, then the aliases (#117); the swap is recorded for rollback (#122)
    cutover_seeded(neo4j, seeded, log=log, store=store, group_id=group.id)
    result["members"] = seeded
    take = load.baseline if baseline is None else baseline
    if take:
        result["baseline"] = baseline_after_restore(
            run_admin, neo4j, store, runner, layout, group, seeded, upload=upload,
            staging=staging, log=log)
    return result


def notify(group, plan: dict, *, log=_NOLOG, sleep=None, now=None) -> dict | None:
    """The load notice (#126): call `load.notice.hook` with the group, tenant, members, the
    scheduled swap time (now + `before`) and what will be lost, then wait `before`. A failed hook
    is logged and the load goes on, unless `required`."""
    import time
    from datetime import datetime, timedelta, timezone

    from .cutover import call_hook

    n = getattr(group.load, "notice", None)
    if n is None:
        return None
    t0 = now or datetime.now(timezone.utc)
    swap_at = (t0 + timedelta(seconds=n.before_seconds)).isoformat()
    payload = {"group": group.id, "tenant": getattr(group, "tenant", None),
               "members": [m["member"] for m in plan["members"]], "swap_at": swap_at,
               "loses": plan["members"][0]["loses"] if plan["members"] else None,
               "mode": group.load.mode}
    status = {"hook": n.hook, "swap_at": swap_at, "sent": False}
    try:
        call_hook(n.hook, payload, env_prefix="NOTICE_")
        status["sent"] = True
        log(f"notice sent for {group.id}: swap at {swap_at}")
    except Exception as e:  # noqa: BLE001 - notifying must not take the load down by default
        status["error"] = f"{type(e).__name__}: {e}"
        log(f"notice for {group.id} failed: {status['error']}")
        if n.required:
            raise OpError(f"notice hook failed and notice.required is set: {status['error']}")
    if n.before_seconds:
        (sleep or time.sleep)(n.before_seconds)
    return status


def build_env(build) -> dict:
    """The command's `env`, with `secret:<ref>` values resolved through the one secret-reference
    syntax (#105)."""
    from .secrets import resolve_ref

    return {k: resolve_ref(v[len("secret:"):]) if v.startswith("secret:") else v
            for k, v in build.env.items()}


def _command_build(run_admin, runner, store, src_store, group, name, local, sp, prefix, ts):
    """A `kind: command` build (#109). `command` runs in the runner in one step with the import
    and dump (its work dir lives only as long as that step). `image` runs as its own pod (k8s
    runner modes) and must write its output to OUTPUT_URI in the store; then the built-in
    import (or a copy of its dump) runs from there."""
    b = group.load.build
    out_prefix = f"{group.id}/_build/{ts}/{naming.slug(name)}/"
    work = f"{runner.scratch_path.rstrip('/')}/_build/{group.id}-{naming.slug(name)}-{ts}"
    env = {"SOURCE_URI": src_store.uri(sp), "OUTPUT_URI": store.uri(out_prefix),
           "WORK_DIR": work, "NEO4J_BACKUP_GROUP": group.id, "NEO4J_BACKUP_MEMBER": name,
           **build_env(b)}
    env.update(store.admin_env() if hasattr(store, "admin_env") else {})
    if b.image:
        run_admin([*b.command, *b.args], env=env, image=b.image)  # writes to OUTPUT_URI
        if b.output == "dump":
            raise OpError("an image build with output 'dump' is not supported yet: write CSV or "
                          "Parquet to OUTPUT_URI with output 'import'")
        args = resolve_args(b.import_args, lambda p: store.uri(out_prefix + p))
        if b.input_type == "parquet":
            args = ["--input-type=parquet", *args]
        return _admin(run_admin, runner.import_dump_command(local, args, store.uri(prefix)),
                      store)
    # relative files are in the work dir; neo4j-admin doesn't resolve them against the shell's
    # working directory ("Unable to find the parent of the path"), so pass absolute paths
    args = resolve_args(b.import_args, lambda p: f"{work}/{p}")
    if b.input_type == "parquet":
        args = ["--input-type=parquet", *args]
    cmd = runner.command_build_command([*b.command, *b.args], local, work, store.uri(prefix),
                                       args, b.output, b.timeout_seconds)
    return run_admin(cmd, env=env)


def reapply_access(neo4j, group, *, log=_NOLOG) -> list[dict]:
    """Apply `writes` to the databases the group serves now, without a load (#112): for a
    `mode: once` group switched to read-only, which has no next load to apply it."""
    neo4j = _per(neo4j, group.id)
    out = []
    for name in group.names:
        physical = neo4j.resolve_physical(name)
        ro = group.load.writes == "read-only"
        neo4j.set_read_only(physical, ro)
        log(f"{physical}: {'read-only' if ro else 'read-write'}")
        out.append({"member": name, "physical": physical,
                    "access": "read-only" if ro else "read-write"})
    return out


__all__ = ["LoadError", "load_group", "plan_load", "reapply_access", "resolve_args",
           "source_prefix", "backs_up", "_privileges_of"]
