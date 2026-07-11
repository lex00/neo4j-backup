"""Framework-free operation bodies shared by the Dagster/Airflow adapters and the CLI (#58 P1).

Each adapter used to carry its own copy of these (Dagster `definitions.py`, Airflow `upload.py` +
DAGs) — the pipeline/upload legs were duplicated verbatim and the backup/aggregate/verify/prune/
restore logic near-verbatim. That's factored here so all three call one implementation.

The seam is deliberately thin:

- **Execution** is a `run_admin` callable — `run_admin(cmd: list) -> None`, runs one neo4j-admin argv
  to completion and raises on failure. Each adapter binds its own (Dagster Pipes, Airflow subprocess/
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

from . import cutover, metadata, naming

RunAdmin = Callable[[list], None]
Log = Callable[[str], None]

_NOLOG: Log = lambda _msg: None


class OpError(Exception):
    """A backup/restore precondition or execution failed. Adapters translate this to their own
    failure type; the message is operator-facing."""


def _stage(runner, tag: str, name: str, staging: str | None) -> str:
    return f"{(staging or runner.scratch_path).rstrip('/')}/{tag}/{name}"


# --- neo4j-admin legs: admin (direct to s3://) vs pipeline (local + boto3 SSE-KMS upload) ------
# `pipeline` exists because neo4j-admin cannot send an SSE-KMS header on its S3 writes; for a
# bucket that denies header-less PutObject the pipeline runs neo4j-admin against local paths and
# lets the object store do the writes. Reads (`--from-path s3://…`) stay direct either way (#39).

def run_backup(run_admin: RunAdmin, runner, store, database: str, prefix: str, kind: str,
               *, upload: str = "admin", staging: str | None = None) -> str | None:
    """Back up `database` into `prefix`; return the artifact key."""
    if upload == "pipeline":
        stage = _stage(runner, "_stage", database, staging)
        os.makedirs(stage, exist_ok=True)
        run_admin(runner.backup_command(database, stage, kind=kind))
        return store.upload_backups(stage, prefix)  # SSE-KMS PUT, then remove local
    run_admin(runner.backup_command(database, store.uri(prefix), kind=kind))
    return store.latest_artifact_key(prefix)


def run_aggregate(run_admin: RunAdmin, runner, store, physical: str, prefix: str,
                  *, upload: str = "admin", staging: str | None = None) -> str | None:
    """Collapse the chain at `prefix` in place; return the recovered-full key."""
    if upload == "pipeline":
        stage = _stage(runner, "_agg", physical, staging)
        store.download_prefix(prefix, stage)
        run_admin(runner.aggregate_command(physical, stage))
        return store.sync_up(stage, prefix)
    run_admin(runner.aggregate_command(physical, store.uri(prefix)))
    return store.latest_artifact_key(prefix)


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

def backup_target(run_admin: RunAdmin, neo4j, store, runner, layout, group_id: str, alias: str,
                  kind: str, *, upload: str = "admin", staging: str | None = None) -> dict:
    """Back up the physical the alias currently targets (or `alias` itself if it's a physical
    name) into that physical's own prefix, so repeated backups form one chain."""
    physical = neo4j.resolve_physical(alias)
    if not physical:
        raise OpError(f"{alias!r} resolves to no physical database — bootstrap the group first")
    prefix = layout.physical_prefix(group_id, alias, physical)
    artifact = run_backup(run_admin, runner, store, physical, prefix, kind,
                          upload=upload, staging=staging)
    return {"group": group_id, "alias": alias, "physical": physical, "kind": kind,
            "artifact": artifact}


def head_physical(store, layout, group_id: str, alias: str) -> tuple[str, str]:
    """The alias's chain head key and its physical name, or raise if the alias has no backup."""
    head = store.latest_artifact_key(layout.alias_prefix(group_id, alias))
    if not head:
        raise OpError(f"no artifact for {group_id}/{alias}")
    return head, layout.physical_of_key(group_id, alias, head)


def aggregate_target(run_admin: RunAdmin, store, runner, layout, group_id: str, alias: str,
                     *, upload: str = "admin", staging: str | None = None) -> dict:
    """Collapse the live physical's chain into one recovered full, IN PLACE (RTO/retention)."""
    _head, physical = head_physical(store, layout, group_id, alias)
    prefix = layout.physical_prefix(group_id, alias, physical)
    full = run_aggregate(run_admin, runner, store, physical, prefix, upload=upload, staging=staging)
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


# --- retention prune (boto3 only) --------------------------------------------------------------

def prune(store, policy, layout, *, keep_system: int = 14, keep_metadata: int = 14,
          dry_run: bool = False) -> dict:
    """Delete `*.backup` older than each group's retention_days, keeping the newest per alias
    (chain head); keep the newest `keep_metadata` DBMS metadata exports and `keep_system` system
    fulls. Chain-naive — aggregate an old chain into a full first to preserve PITR coverage.
    `dry_run` enumerates the victims (`keys`) without deleting — the CLI's blast radius."""
    now = datetime.now(timezone.utc)
    victims: list[str] = []
    detail: dict[str, int] = {}

    def _sweep(keys, label):
        keys = list(keys)
        if not keys:
            return
        detail[label] = len(keys)
        victims.extend(keys)
        if not dry_run:
            store.delete_keys(keys)

    for g in policy.db_groups:
        cutoff = now - timedelta(days=g.retention_days)
        for a in g.names:
            arts = store.list_artifacts(layout.alias_prefix(g.id, a))
            if not arts:
                continue
            newest = max(arts, key=lambda t: t[2])[0]
            _sweep([k for (k, _s, m) in arts if m < cutoff and k != newest], f"{g.id}/{a}")
    # DBMS metadata: keep the newest N (mirrors metadata.prune, but enumerable for dry-run)
    marts = sorted(store.list_text_keys(layout.metadata_prefix()), key=lambda t: t[1])
    _sweep([k for (k, _m) in (marts[:-keep_metadata] if keep_metadata > 0 else marts)],
           "_dbms/metadata")
    sysarts = sorted(store.list_artifacts(layout.system_prefix()), key=lambda t: t[2])
    _sweep([k for (k, _s, _m) in sysarts[:-keep_system]], "_dbms/system")
    return {"deleted": len(victims), "detail": detail, "keys": victims, "dry_run": dry_run}


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

def seed_member(neo4j, store, group, layout, name: str, *, restore_until: str | None = None,
                replace: bool = False, cypher_version: str | None = None, ts: str | None = None,
                log: Log = _NOLOG) -> dict:
    """Seed one group member from its latest artifact. by-name: create-if-absent, or DROP+recreate
    an existing target with `replace` (destructive; Neo4j has no rename). alias-swap: seed a fresh
    physical named for `ts` (one group-wide timestamp; defaults to now) — the caller cuts the alias
    over afterwards. Returns a plan entry tagged with `mode`."""
    key = store.latest_artifact_key(layout.alias_prefix(group.id, name))
    if not key:
        raise OpError(f"no artifact for {group.id}/{name} — back up first")
    if group.restore_mode == "by-name":
        existed = neo4j.database_exists(name)
        if existed and not replace:
            raise OpError(f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
        if existed:
            neo4j.drop_database(name)
            log(f"dropped {name} (replace)")
        neo4j.seed_database(name, store.uri(key), restore_until=restore_until,
                            topology=group.topology_for(name), cypher_version=cypher_version)
        log(f"restored {name} <= {key}")
        return {"mode": "by-name", "name": name, "key": key, "replaced": existed}
    old = neo4j.alias_target(name)  # captured before cutover (for external routing #17)
    newdb = naming.physical(name, ts or naming.ts())
    neo4j.seed_database(newdb, store.uri(key), restore_until=restore_until,
                        topology=group.topology_for(name), cypher_version=cypher_version)
    log(f"seeded {newdb} <= {key}")
    return {"mode": "alias-swap", "alias": name, "newdb": newdb, "old": old, "key": key}


def cutover_seeded(neo4j, seeded: list[dict], *, log: Log = _NOLOG) -> None:
    """Barrier after alias-swap seeds: point each alias at its new physical. by-name entries are
    no-ops (already restored under their own name)."""
    strategy = cutover.from_env()  # alias-swap (default) or external routing (#17)
    for s in seeded:
        if s.get("mode") == "by-name":
            continue
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
            if existed:
                neo4j.drop_database(name)
                log(f"dropped {name} (replace)")
            neo4j.seed_database(name, store.uri(key), restore_until=restore_until,
                                topology=group.topology_for(name), cypher_version=cypher_version)
            log(f"restored {name} <= {key}")
            members.append({"mode": "by-name", "name": name, "key": key, "replaced": existed})
        return {"mode": "by-name", "members": members}
    ts = naming.ts()  # one timestamp for every physical seeded in this group restore
    for name in group.names:
        members.append(seed_member(neo4j, store, group, layout, name, restore_until=restore_until,
                                   cypher_version=cypher_version, ts=ts, log=log))
    cutover_seeded(neo4j, members, log=log)
    return {"mode": "alias-swap", "members": members}


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
                            "drops": [name] if existed else []})
        else:
            members.append({"mode": "alias-swap", "alias": name, "key": key,
                            "action": "seed new physical, then swap alias", "drops": []})
    return {"mode": group.restore_mode, "members": members,
            "drops": [d for m in members for d in m["drops"]]}
