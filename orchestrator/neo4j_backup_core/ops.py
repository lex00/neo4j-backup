"""Framework-free operation bodies shared by the Dagster/Airflow adapters and the CLI (#58 P1).

Each adapter used to carry its own copy of these (Dagster `definitions.py`, Airflow `upload.py` +
DAGs) — the pipeline/upload legs were duplicated verbatim and the backup/aggregate/verify/prune/
restore logic near-verbatim. That's factored here so all three call one implementation.

The seam is deliberately thin:

- **Execution** is a `run_admin` callable — `run_admin(cmd: list) -> str`, runs one neo4j-admin argv
  to completion, raises on failure, and returns the command's output (#118). Each adapter binds its own (Dagster Pipes, Airflow subprocess/
  KPO, CLI subprocess), including how the environment is applied, so this module never touches
  process/pod machinery.
- **Handles** (`neo4j`, `store`, `runner`) are the core clients; `layout` is a `paths.PathLayout`.
- **Config** (`upload`, `staging`, `restore_until`, `replace`, `cypher_version`) is passed in, not
  read from the environment — so the ops stay pure and unit-testable.
- **Failure** is `OpError`; adapters map it to their own type (`dg.Failure` / `RuntimeError` /
  the CLI's `Exit.FAILURE`). **Logging** is an optional `log(msg)` callable, default no-op.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Callable

from . import artifacts, cutover, metadata, naming, privileges

RunAdmin = Callable[[list], None]
Log = Callable[[str], None]

_NOLOG: Log = lambda _msg: None


class OpError(Exception):
    """A backup/restore precondition or execution failed. Adapters translate this to their own
    failure type; the message is operator-facing."""


class BaselineError(OpError):
    """The restore was applied but its baseline full backup failed (#97). `done` lists the
    baselines that were taken; rerun `backup <group> --kind FULL` for the rest."""

    kind = "baseline_failed"

    def __init__(self, msg: str, done: list[dict]):
        super().__init__(msg)
        self.done = done


def _stage(runner, tag: str, name: str, staging: str | None) -> str:
    return f"{(staging or runner.scratch_path).rstrip('/')}/{tag}/{name}"


# --- neo4j-admin legs: admin (direct to s3://) vs pipeline (local + boto3 SSE-KMS upload) ------
# `pipeline` exists because neo4j-admin cannot send an SSE-KMS header on its S3 writes; for a
# bucket that denies header-less PutObject the pipeline runs neo4j-admin against local paths and
# lets the object store do the writes. Reads (`--from-path s3://…`) stay direct either way (#39).

def record_artifacts(run_admin: RunAdmin, runner, store, prefix: str, *, rewrite: bool = False,
                     log: Log = _NOLOG) -> dict:
    """Bring the sidecars under `prefix` in step with its artifacts (#118): inspect the folder in
    the store, write a sidecar for every artifact that lacks one (all of them with `rewrite`, after
    an aggregate changed the chain), and delete sidecars whose artifact is gone. Not fatal: a
    failure is logged and returned, and the artifacts stay `unknown` until the next run backfills
    them."""
    try:
        _metas, unknown = artifacts.read(store, prefix)
        want = None if rewrite else {k[len(prefix):] for k in unknown}
        written = []
        if want is None or want:
            out = run_admin(runner.inspect_command(store.uri(prefix)))
            written = artifacts.write_sidecars(store, prefix, out, only=want)
        dropped = artifacts.drop_orphans(store, prefix)
        return {"written": len(written), "dropped": len(dropped)}
    except Exception as e:  # noqa: BLE001 - the artifact itself is fine; report and move on
        log(f"warning: could not record artifact metadata under {prefix}: {e}")
        return {"error": str(e)}


def run_backup(run_admin: RunAdmin, runner, store, database: str, prefix: str, kind: str,
               *, upload: str = "admin", staging: str | None = None,
               log: Log = _NOLOG) -> str | None:
    """Back up `database` into `prefix`, record its sidecar; return the artifact key."""
    if upload == "pipeline":
        stage = _stage(runner, "_stage", database, staging)
        os.makedirs(stage, exist_ok=True)
        run_admin(runner.backup_command(database, stage, kind=kind))
        key = store.upload_backups(stage, prefix)  # SSE-KMS PUT, then remove local
    else:
        run_admin(runner.backup_command(database, store.uri(prefix), kind=kind))
        key = store.latest_artifact_key(prefix)
    record_artifacts(run_admin, runner, store, prefix, log=log)  # reads stay direct (#39)
    return key


def run_aggregate(run_admin: RunAdmin, runner, store, physical: str, prefix: str,
                  *, upload: str = "admin", staging: str | None = None,
                  log: Log = _NOLOG) -> str | None:
    """Collapse the chain at `prefix` in place, rewrite its sidecars; return the recovered-full
    key."""
    if upload == "pipeline":
        stage = _stage(runner, "_agg", physical, staging)
        store.download_prefix(prefix, stage)
        run_admin(runner.aggregate_command(physical, stage))
        key = store.sync_up(stage, prefix)
    else:
        run_admin(runner.aggregate_command(physical, store.uri(prefix)))
        key = store.latest_artifact_key(prefix)
    record_artifacts(run_admin, runner, store, prefix, rewrite=True, log=log)
    return key


def run_verify(run_admin: RunAdmin, runner, store, physical: str, src: str, scratch: str,
               *, upload: str = "admin", staging: str | None = None) -> int:
    """Non-destructively prove `src` is restorable: copy it aside, aggregate the copy into a
    recovered full, `database check` it, then clean up. Return the artifact count checked."""
    if upload == "pipeline":
        stage = _stage(runner, "_verify", physical, staging)
        copied = store.download_prefix(src, stage)  # verify on local disk — no S3 scratch writes
        try:
            run_admin(runner.aggregate_command(physical, stage))
            full = next(f for f in sorted(os.listdir(stage)) if f.endswith(".backup"))
            run_admin(runner.check_command(physical, os.path.join(stage, full)))
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        return copied
    try:
        copied = store.copy_prefix(src, scratch)
        run_admin(runner.aggregate_command(physical, store.uri(scratch)))
        full = store.latest_artifact_key(scratch)
        run_admin(runner.check_command(physical, store.uri(full)))
    finally:
        store.delete_prefix(scratch)
    return copied


# --- target-level ops: resolve the target, then run the right leg ------------------------------

def base_full(neo4j, store, layout, group_id: str, alias: str) -> dict:
    """Whether the database the alias points at has a full backup of its current store to diff
    against (#74). A seed (alias-swap restore, import, adopt) starts a new physical with an empty
    folder, and a by-name DROP + CREATE starts a new store under the old name, so only a full taken
    after the database's creation counts (sidecar times, #118). Returns `physical`, `full` (the
    newest such full's key, or None) and `unknown` (backups in the folder without a sidecar, which
    might be a base; callers treat those as "maybe")."""
    physical = neo4j.resolve_physical(alias)
    if not physical:
        raise OpError(f"{alias!r} resolves to no physical database — bootstrap the group first")
    prefix = layout.physical_prefix(group_id, alias, physical)
    metas, unknown = artifacts.read(store, prefix)
    created = neo4j.database_created(physical)
    fulls = sorted((m.get("time") or "", k) for k, m in metas.items()
                   if m.get("type") == "full"
                   and (created is None or _sidecar_time(m.get("time")) >= _floor(created)))
    return {"physical": physical, "prefix": prefix, "full": fulls[-1][1] if fulls else None,
            "unknown": len(unknown)}


def _floor(t: datetime) -> datetime:
    # sidecar times have whole seconds; compare at that precision
    t = t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    return t.replace(microsecond=0)


def needs_full(neo4j, store, layout, group_id: str, alias: str) -> bool:
    """True when a DIFF of this member would fail for want of a base full (#74). Unknown
    (pre-#118) backups count as a possible base, so this never forces a full on a guess."""
    b = base_full(neo4j, store, layout, group_id, alias)
    return b["full"] is None and b["unknown"] == 0


def backup_target(run_admin: RunAdmin, neo4j, store, runner, layout, group_id: str, alias: str,
                  kind: str, *, upload: str = "admin", staging: str | None = None,
                  log: Log = _NOLOG, physical: str | None = None) -> dict:
    """Back up the physical the alias currently targets (or `alias` itself if it's a physical
    name) into that physical's own prefix, so repeated backups form one chain. A DIFF with no
    full of the current store to diff against is skipped, not failed: the result has
    `skipped: true, needs_full: true` and the caller runs a FULL in the full lane (#74). `taken`
    is the type neo4j-admin actually wrote (an AUTO can be either), from the sidecar. `physical`
    names the database explicitly (a restore's new physical, even when an external router, not
    the alias, points at it)."""
    physical = physical or neo4j.resolve_physical(alias)
    if not physical:
        raise OpError(f"{alias!r} resolves to no physical database — bootstrap the group first")
    prefix = layout.physical_prefix(group_id, alias, physical)
    if kind.upper() == "DIFF":
        if artifacts.read(store, prefix)[1]:  # pre-#118 backups: inspect so the check can see them
            record_artifacts(run_admin, runner, store, prefix, log=log)
        if needs_full(neo4j, store, layout, group_id, alias):
            log(f"{group_id}/{alias}: no full backup of {physical} to diff against; "
                f"skipping the diff, a full is needed")
            return {"group": group_id, "alias": alias, "physical": physical, "kind": kind,
                    "artifact": None, "taken": None, "skipped": True, "needs_full": True}
    artifact = run_backup(run_admin, runner, store, physical, prefix, kind,
                          upload=upload, staging=staging, log=log)
    metas, _unknown = artifacts.read(store, prefix)
    taken = (metas.get(artifact) or {}).get("type") if artifact else None
    return {"group": group_id, "alias": alias, "physical": physical, "kind": kind,
            "artifact": artifact, "taken": taken, "skipped": False, "needs_full": False}


def head_physical(store, layout, group_id: str, alias: str) -> tuple[str, str]:
    """The alias's chain head key and its physical name, or raise if the alias has no backup."""
    head = store.latest_artifact_key(layout.alias_prefix(group_id, alias))
    if not head:
        raise OpError(f"no artifact for {group_id}/{alias}")
    return head, layout.physical_of_key(group_id, alias, head)


def aggregate_target(run_admin: RunAdmin, store, runner, layout, group_id: str, alias: str,
                     *, upload: str = "admin", staging: str | None = None,
                     log: Log = _NOLOG) -> dict:
    """Collapse the live physical's chain into one recovered full, IN PLACE (RTO/retention)."""
    _head, physical = head_physical(store, layout, group_id, alias)
    prefix = layout.physical_prefix(group_id, alias, physical)
    full = run_aggregate(run_admin, runner, store, physical, prefix, upload=upload, staging=staging,
                         log=log)
    return {"alias": alias, "physical": physical, "full": full}


def verify_target(run_admin: RunAdmin, store, runner, layout, group_id: str, alias: str,
                  *, upload: str = "admin", staging: str | None = None) -> dict:
    """Non-destructive consistency check of the latest backup for the alias."""
    _head, physical = head_physical(store, layout, group_id, alias)
    src = layout.physical_prefix(group_id, alias, physical)
    scratch = f"_verify/{group_id}/{physical}/"
    checked = run_verify(run_admin, runner, store, physical, src, scratch,
                         upload=upload, staging=staging)
    return {"alias": alias, "physical": physical, "consistent": True, "checked": checked}


def artifact_chains(store, layout, group_id: str, alias: str, *, run_admin: RunAdmin | None = None,
                    runner=None, log: Log = _NOLOG) -> dict:
    """The backup chains for one member, per physical, from the sidecars (#118). With `run_admin`
    and `runner`, artifacts that have no sidecar (taken before #118) are inspected once and
    backfilled first; without, they are reported under `unknown`."""
    prefix = layout.alias_prefix(group_id, alias)
    if run_admin is not None and runner is not None:
        _metas, unknown = artifacts.read(store, prefix)
        for folder in sorted({k.rsplit("/", 1)[0] + "/" for k in unknown}):
            record_artifacts(run_admin, runner, store, folder, log=log)
    return artifacts.chains(store, prefix)


def import_database(run_admin: RunAdmin, runner, database: str, source_args: list) -> dict:
    """Bulk-import raw data into an offline store on the loader (#16): the reusable tail's first step.
    Structures + runs `neo4j-admin database import full`; `source_args` is a passthrough. The rest of
    the tail (start Neo4j → CREATE DATABASE → backup → verify → seed) is existing commands — see
    IMPORT.md. No store/neo4j handle: import is local to the loader, not a network/object-store op."""
    cmd = runner.import_command(database, source_args)
    run_admin(cmd)
    return {"database": database, "argv": cmd}


def system_backup(run_admin: RunAdmin, store, runner, layout,
                  *, upload: str = "admin", staging: str | None = None) -> dict:
    """Binary FULL backup of the `system` database to the reserved `_dbms/system/` prefix (#15)."""
    prefix = layout.system_prefix()
    key = run_backup(run_admin, runner, store, "system", prefix, "FULL",
                     upload=upload, staging=staging)
    return {"key": key}


# --- retention prune --------------------------------------------------------------------------

def _sidecar_time(t: str | None) -> datetime | None:
    # neo4j-admin reports UTC without an offset, e.g. 2026-11-15T21:26:56
    if not t:
        return None
    d = datetime.fromisoformat(t)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def plan_member_prune(chains: list[dict], cutoff: datetime) -> tuple[list[str], int]:
    """Which artifacts of one member to delete, chain by chain (#73). `chains` is
    `artifacts.chains(...)["chains"]` for the member (every physical and store). Kept: the newest
    chain; every chain whose newest artifact is at or after the cutoff; and every chain from the
    newest full before the cutoff onward (it covers the cutoff instant). The rest are deleted
    whole. Returns (keys to delete, number of kept artifacts older than the cutoff)."""
    if not chains:
        return [], 0
    starts = [_sidecar_time(c["from"]) for c in chains]
    ends = [_sidecar_time(c["to"]) for c in chains]
    newest = max(range(len(chains)), key=lambda i: ends[i])
    before = [st for st in starts if st < cutoff]
    anchor = max(before) if before else None
    delete: list[str] = []
    held = 0
    for i, c in enumerate(chains):
        keep = (i == newest or ends[i] >= cutoff or (anchor is not None and starts[i] >= anchor))
        if keep:
            held += sum(1 for a in c["artifacts"] if _sidecar_time(a["time"]) < cutoff)
        else:
            delete.extend(a["key"] for a in c["artifacts"])
    return delete, held


def keep_count(name: str, value) -> int:
    """How many of the newest DBMS backups to keep: an integer >= 1 (#78). 0 meant "delete none"
    for system fulls and "delete all" for metadata exports; neither is a retention setting."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise OpError(f"{name} must be an integer >= 1 (the newest N are kept), got {value!r}")
    return value


def prune(store, policy, layout, *, keep_system: int = 14, keep_metadata: int = 14,
          dry_run: bool = False, run_admin: RunAdmin | None = None, runner=None,
          now: datetime | None = None, log: Log = _NOLOG) -> dict:
    """Delete backups past each group's `retention_days` without breaking a chain (#73): per
    member, whole chains are deleted only when nothing kept depends on them
    (`plan_member_prune`). Full or diff is read from the sidecars (#118). Artifacts with no sidecar
    are inspected first when `run_admin`/`runner` are given (this writes sidecars even on a dry
    run, so the preview and the real prune see the same chains); otherwise they are kept and
    counted in `unknown_kept`. Also keeps the newest `keep_metadata` DBMS metadata exports and
    `keep_system` system fulls (both integers >= 1, #78). `dry_run` lists the victims (`keys`)
    without deleting."""
    keep_count("keep_system", keep_system)
    keep_count("keep_metadata", keep_metadata)
    now = now or datetime.now(timezone.utc)
    victims: list[str] = []
    detail: dict[str, int] = {}
    held_total = 0
    unknown_kept: list[str] = []

    def _sweep(keys, label):
        keys = list(keys)
        if not keys:
            return
        detail[label] = len(keys)
        victims.extend(keys)
        if not dry_run:
            store.delete_keys(keys)

    swept: list[str] = []
    for g in policy.db_groups:
        cutoff = now - timedelta(days=g.retention_days)
        for a in g.names:
            prefix = layout.alias_prefix(g.id, a)
            if not store.list_artifacts(prefix):
                continue
            read = artifact_chains(store, layout, g.id, a, run_admin=run_admin, runner=runner,
                                   log=log)
            keys, held = plan_member_prune(read["chains"], cutoff)
            held_total += held
            unknown_kept.extend(read["unknown"])
            _sweep(keys, f"{g.id}/{a}")
            swept.append(prefix)
    # DBMS metadata: keep the newest N (mirrors metadata.prune, but enumerable for dry-run)
    marts = sorted(store.list_text_keys(layout.metadata_prefix()), key=lambda t: t[1])
    _sweep([k for (k, _m) in marts[:-keep_metadata]], "_dbms/metadata")
    sysarts = sorted(store.list_artifacts(layout.system_prefix()), key=lambda t: t[2])
    _sweep([k for (k, _s, _m) in sysarts[:-keep_system]], "_dbms/system")
    # sidecars (#118) go with their artifacts
    sidecars = 0
    if not dry_run:
        for prefix in [*swept, layout.system_prefix()]:
            sidecars += len(artifacts.drop_orphans(store, prefix))
    return {"deleted": len(victims), "detail": detail, "keys": victims, "dry_run": dry_run,
            "sidecars_deleted": sidecars, "kept_for_chains": held_total,
            "unknown_kept": len(unknown_kept)}


# --- DBMS metadata (pure Bolt, no runner) ------------------------------------------------------

def export_metadata(neo4j, store, layout) -> dict:
    """Capture the DBMS security + alias layer as replayable Cypher under `_dbms/` (#14)."""
    ts = naming.ts()
    key = layout.metadata_key(ts)
    cypher = metadata.render(metadata.capture(neo4j), ts=ts)
    store.put_text(key, cypher)
    return {"key": key, "bytes": len(cypher)}


def restore_metadata(neo4j, store, layout, key: str | None = None) -> dict:
    """Replay a metadata export (latest, or a given `key`) against `system` over Bolt."""
    key = key or store.latest_text_key(layout.metadata_prefix())
    if not key:
        raise OpError("no metadata artifact — export first")
    result = metadata.replay(neo4j, store.get_text(key))
    return {"key": key, **result}


# --- restore (pure Cypher): alias-swap (default) or by-name (#48) -------------------------------

def _privileges_of(neo4j, database: str | None) -> list[str]:
    """Privileges scoped to `database`, or [] when there is none (#117)."""
    return neo4j.privilege_commands(database) if database else []


def regrant(neo4j, commands: list[str], *, old: str | None = None, new: str | None = None,
            log: Log = _NOLOG) -> list[str]:
    """Apply `commands`, moving their ON clause from `old` to `new` when both are given. Stops at
    the first failure with an OpError naming the commands that were not applied (#117)."""
    applied: list[str] = []
    for cmd in commands:
        cmd = privileges.rewrite(cmd, old, new) if old and new else cmd
        try:
            neo4j.run_system(cmd)
        except Exception as e:  # noqa: BLE001 - any driver error stops the restore
            rest = [privileges.rewrite(c, old, new) if old and new else c
                    for c in commands[len(applied):]]
            raise OpError(f"privilege grant failed ({e}); not applied: {rest}") from e
        applied.append(cmd)
    if applied:
        log(f"granted {len(applied)} privilege(s) on {new or '(same name)'}")
    return applied


def _restore_by_name(neo4j, group, name: str, key: str, existed: bool, *, store,
                     restore_until, cypher_version, log: Log) -> dict:
    """DROP (if present) then CREATE under the same name. DROP removes the database's privileges,
    so they are captured first and re-granted after the create (#117)."""
    held = _privileges_of(neo4j, name) if existed else []
    if existed:
        neo4j.drop_database(name)
        log(f"dropped {name} (replace)")
    neo4j.seed_database(name, store.uri(key), restore_until=restore_until,
                        topology=group.topology_for(name), cypher_version=cypher_version,
                        store_base=store.uri(""))
    log(f"restored {name} <= {key}")
    try:
        granted = regrant(neo4j, held, log=log)
    except OpError as e:
        raise OpError(f"{name} restored from {key}, but its privileges were not all re-granted: "
                      f"{e}") from e
    return {"mode": "by-name", "name": name, "key": key, "replaced": existed,
            "privileges": granted}


def seed_member(neo4j, store, group, layout, name: str, *, restore_until: str | None = None,
                replace: bool = False, cypher_version: str | None = None, ts: str | None = None,
                log: Log = _NOLOG) -> dict:
    """Seed one group member from its latest artifact. by-name: create-if-absent, or DROP+recreate
    an existing target with `replace` (destructive; Neo4j has no rename), keeping its privileges.
    alias-swap: seed a fresh physical named for `ts` (one group-wide timestamp; defaults to now) —
    the caller cuts the alias over afterwards (`cutover_seeded`, which copies privileges first).
    Returns a plan entry tagged with `mode`."""
    key = store.latest_artifact_key(layout.alias_prefix(group.id, name))
    if not key:
        raise OpError(f"no artifact for {group.id}/{name} — back up first")
    if group.restore_mode == "by-name":
        existed = neo4j.database_exists(name)
        if existed and not replace:
            raise OpError(f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
        return _restore_by_name(neo4j, group, name, key, existed, store=store,
                                restore_until=restore_until, cypher_version=cypher_version, log=log)
    old = neo4j.alias_target(name)  # captured before cutover (for external routing #17)
    newdb = naming.physical(name, ts or naming.ts())
    neo4j.seed_database(newdb, store.uri(key), restore_until=restore_until,
                        topology=group.topology_for(name), cypher_version=cypher_version,
                        store_base=store.uri(""))
    log(f"seeded {newdb} <= {key}")
    return {"mode": "alias-swap", "alias": name, "newdb": newdb, "old": old, "key": key}


def cutover_seeded(neo4j, seeded: list[dict], *, log: Log = _NOLOG) -> None:
    """Barrier after alias-swap seeds. First copy every member's privileges from its old physical
    to its new one (#117); only when all copies succeed, point each alias at its new physical. A
    failed copy raises before any alias moves. Privileges on the old physical are left in place
    (a rollback needs them; DROP DATABASE removes them). by-name entries are no-ops here."""
    swaps = [s for s in seeded if s.get("mode") != "by-name"]
    for s in swaps:
        try:
            s["privileges"] = regrant(neo4j, _privileges_of(neo4j, s.get("old")),
                                      old=s.get("old"), new=s["newdb"], log=log)
        except OpError as e:
            raise OpError(f"{s['alias']}: copying privileges {s.get('old')} -> {s['newdb']} "
                          f"failed, no alias was moved: {e}") from e
    strategy = cutover.from_env()  # alias-swap (default) or external routing (#17)
    for s in swaps:
        strategy.cutover(neo4j, s["alias"], s["newdb"], s.get("old"))
        log(f"cutover {s['alias']}: {s.get('old')} -> {s['newdb']}")


def restore_group(neo4j, store, group, layout, *, restore_until: str | None = None,
                  replace: bool = False, cypher_version: str | None = None, log: Log = _NOLOG) -> dict:
    """Restore a whole group in one process (used by the CLI and Dagster; Airflow maps `seed_member`
    across tasks instead). by-name pre-validates every artifact + existing-target precondition BEFORE
    any drop, so a missing artifact or an unreplaceable target fails before anything is destroyed.
    alias-swap seeds every fresh physical, then cuts all aliases over. Returns the member plan."""
    members: list[dict] = []
    if group.restore_mode == "by-name":
        plan: list[tuple[str, str, bool]] = []
        for name in group.names:
            key = store.latest_artifact_key(layout.alias_prefix(group.id, name))
            if not key:
                raise OpError(f"no artifact for {group.id}/{name} — back up first")
            existed = neo4j.database_exists(name)
            if existed and not replace:
                raise OpError(
                    f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
            plan.append((name, key, existed))
        for name, key, existed in plan:
            members.append(_restore_by_name(neo4j, group, name, key, existed, store=store,
                                            restore_until=restore_until,
                                            cypher_version=cypher_version, log=log))
        return {"mode": "by-name", "members": members}
    ts = naming.ts()  # one timestamp for every physical seeded in this group restore
    for name in group.names:
        members.append(seed_member(neo4j, store, group, layout, name, restore_until=restore_until,
                                   cypher_version=cypher_version, ts=ts, log=log))
    cutover_seeded(neo4j, members, log=log)
    return {"mode": "alias-swap", "members": members}


def baseline_after_restore(run_admin: RunAdmin, neo4j, store, runner, layout, group,
                           members: list[dict], *, upload: str = "admin",
                           staging: str | None = None, log: Log = _NOLOG) -> list[dict]:
    """A FULL backup of every database a restore created (#97), so the next diff has a base
    (#74) and point-in-time coverage of the new database starts now rather than at the next full
    schedule. alias-swap members are backed up by their new physical; by-name members by name.
    Raises BaselineError (the restore stays applied) at the first failure."""
    done: list[dict] = []
    for m in members:
        alias = m.get("alias") or m["name"]
        physical = m.get("newdb") or m["name"]
        try:
            out = backup_target(run_admin, neo4j, store, runner, layout, group.id, alias, "FULL",
                                upload=upload, staging=staging, log=log, physical=physical)
        except Exception as e:  # noqa: BLE001 - any failure: the restore itself is done
            raise BaselineError(
                f"restore applied, but the baseline full of {physical} failed ({e}); run "
                f"`neo4j-backup backup {group.id} --kind FULL`", done) from e
        done.append({"member": alias, "physical": physical, "artifact": out["artifact"],
                     "taken": out["taken"]})
        log(f"baseline {physical} -> {out['artifact']}")
    return done


def plan_restore(neo4j, store, group, layout, *, replace: bool = False) -> dict:
    """The blast radius of `restore_group` WITHOUT mutating: per member, the artifact that would be
    used and what would happen — by-name `create` or `drop+recreate`, alias-swap seed + swap. Runs
    the same preconditions (missing artifact, unreplaceable existing target) so a dry-run fails the
    same way a real restore would. `drops` is the destructive surface (databases to be DROPped)."""
    members: list[dict] = []
    for name in group.names:
        key = store.latest_artifact_key(layout.alias_prefix(group.id, name))
        if not key:
            raise OpError(f"no artifact for {group.id}/{name} — back up first")
        if group.restore_mode == "by-name":
            existed = neo4j.database_exists(name)
            if existed and not replace:
                raise OpError(
                    f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
            members.append({"mode": "by-name", "name": name, "key": key,
                            "action": "drop+recreate" if existed else "create",
                            "drops": [name] if existed else [],
                            # captured before the drop, re-granted after the create (#117)
                            "privileges": _privileges_of(neo4j, name) if existed else []})
        else:
            old = neo4j.alias_target(name)
            members.append({"mode": "alias-swap", "alias": name, "key": key, "old": old,
                            "action": "seed new physical, copy privileges, then swap alias",
                            "drops": [],
                            # copied from the old physical to the new one before the swap (#117)
                            "privileges": _privileges_of(neo4j, old)})
    return {"mode": group.restore_mode, "members": members,
            "drops": [d for m in members for d in m["drops"]]}
