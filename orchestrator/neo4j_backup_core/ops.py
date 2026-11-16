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


class SeedError(OpError):
    """A group restore failed while seeding (#140). The members seeded before the failure had
    no alias pointing at them and held no new writes, so they were dropped: `dropped` names them,
    `kept` any that couldn't be dropped."""

    kind = "seed_failed"

    def __init__(self, msg: str, dropped: list[str], kept: list[str]):
        super().__init__(msg)
        self.dropped, self.kept = dropped, kept


def seed_all(neo4j, group, names, seed_one, *, log: Log = _NOLOG) -> list[dict]:
    """`seed_one(name)` for each alias-swap member; if one raises, drop the physicals already
    seeded (nothing points at them yet) and raise SeedError naming them (#140)."""
    seeded: list[dict] = []
    for name in names:
        try:
            seeded.append(seed_one(name))
        except Exception as e:  # noqa: BLE001 - any failure: undo the unswapped seeds
            dropped, kept = [], []
            for s in seeded:
                db = s.get("newdb")
                if not db:
                    continue
                try:
                    neo4j.drop_database(db)
                    dropped.append(db)
                    log(f"dropped {db} (seeded before the failure, never swapped in)")
                except Exception:  # noqa: BLE001
                    kept.append(db)
            raise SeedError(
                f"{group.id}/{name}: seeding failed ({type(e).__name__}: {e}); no alias moved"
                + (f"; dropped the members already seeded: {dropped}" if dropped else "")
                + (f"; couldn't drop {kept}, drop them yourself" if kept else ""),
                dropped, kept) from e
    return seeded


class BaselineError(OpError):
    """The restore was applied but its baseline full backup failed (#97). `done` lists the
    baselines that were taken; rerun `backup <group> --kind FULL` for the rest."""

    kind = "baseline_failed"

    def __init__(self, msg: str, done: list[dict]):
        super().__init__(msg)
        self.done = done


def _store(store, group_id: str):
    """The handle for `group_id`: a StoreSet resolves the group's named store (#105), a Neo4jSet
    or RunnerSet its cluster's client or runner (#106); a plain handle (tests, single-cluster
    callers) is used as is."""
    return store.for_group(group_id) if hasattr(store, "for_group") else store


_per = _store


def _cypher(neo4j, cypher_version):
    return getattr(neo4j, "cypher_version", None) or cypher_version


def _upload(runner, upload):
    return getattr(runner, "backup_upload", None) or upload


def _topology(neo4j, group, name):
    return group.topology_for(name) or getattr(neo4j, "cluster_topology", None)


def _admin(run_admin: RunAdmin, cmd: list, store) -> str:
    """Run a neo4j-admin command that reads or writes `store`, with the store's own settings
    (region, runner endpoint, credentials) in its environment when it has any (#105)."""
    env = store.admin_env() if hasattr(store, "admin_env") else {}
    return run_admin(cmd, env=env) if env else run_admin(cmd)


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
            out = _admin(run_admin, runner.inspect_command(store.uri(prefix)), store)
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
        _admin(run_admin, runner.backup_command(database, stage, kind=kind), store)
        key = store.upload_backups(stage, prefix)  # SSE-KMS PUT, then remove local
    else:
        _admin(run_admin, runner.backup_command(database, store.uri(prefix), kind=kind), store)
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
        _admin(run_admin, runner.aggregate_command(physical, stage), store)
        key = store.sync_up(stage, prefix)
    else:
        _admin(run_admin, runner.aggregate_command(physical, store.uri(prefix)), store)
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
            _admin(run_admin, runner.aggregate_command(physical, stage), store)
            full = next(f for f in sorted(os.listdir(stage)) if f.endswith(".backup"))
            _admin(run_admin, runner.check_command(physical, os.path.join(stage, full)), store)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        return copied
    try:
        copied = store.copy_prefix(src, scratch)
        _admin(run_admin, runner.aggregate_command(physical, store.uri(scratch)), store)
        full = store.latest_artifact_key(scratch)
        _admin(run_admin, runner.check_command(physical, store.uri(full)), store)
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
    neo4j = _per(neo4j, group_id)
    store = _store(store, group_id)
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


def backs_up(neo4j, group, physical: str) -> dict:
    """Whether a member gets scheduled backups (#112): only while its live database is
    read-write AND the group protects its writes (`protect_writes` true, the default for
    read-write). The live access mode decides, not the policy alone: a database still read-write
    after the policy says read-only keeps getting backups until a load makes it read-only.
    Returns {backs_up, reason}."""
    load = getattr(group, "load", None)
    if (getattr(load, "writes", None) == "read-write"
            and getattr(load, "protect_writes", None) is False):
        return {"backs_up": False, "reason": "protect_writes: false (writes are disposable)"}
    access = neo4j.database_access(physical) if hasattr(neo4j, "database_access") else None
    if access == "read-only":
        return {"backs_up": False, "reason": "the database is read-only (its loads restore it)"}
    return {"backs_up": True, "reason": None}


def apply_access(neo4j, group, physical: str, *, log: Log = _NOLOG) -> str | None:
    """Make a new physical read-only when the group says `writes: read-only` (#112); called after
    the seed and before the swap. Returns the access set, or None when nothing changed."""
    if getattr(getattr(group, "load", None), "writes", None) == "read-only":
        neo4j.set_read_only(physical, True)
        log(f"{physical}: set read-only")
        return "read-only"
    return None


def needs_full(neo4j, store, layout, group_id: str, alias: str) -> bool:
    """True when a DIFF of this member would fail for want of a base full (#74). Unknown
    (pre-#118) backups count as a possible base, so this never forces a full on a guess."""
    b = base_full(neo4j, store, layout, group_id, alias)
    return b["full"] is None and b["unknown"] == 0


def backup_target(run_admin: RunAdmin, neo4j, store, runner, layout, group_id: str, alias: str,
                  kind: str, *, upload: str = "admin", staging: str | None = None,
                  log: Log = _NOLOG, physical: str | None = None, group=None) -> dict:
    """Back up the physical the alias currently targets (or `alias` itself if it's a physical
    name) into that physical's own prefix, so repeated backups form one chain. A DIFF with no
    full of the current store to diff against is skipped, not failed: the result has
    `skipped: true, needs_full: true` and the caller runs a FULL in the full lane (#74). `taken`
    is the type neo4j-admin actually wrote (an AUTO can be either), from the sidecar. `physical`
    names the database explicitly (a restore's new physical, even when an external router, not
    the alias, points at it)."""
    check_fence(neo4j, store, group_id)  # moved to another cluster (#115)
    neo4j, runner = _per(neo4j, group_id), _per(runner, group_id)
    upload = _upload(runner, upload)
    store = _store(store, group_id)
    physical = physical or neo4j.resolve_physical(alias)
    if not physical:
        raise OpError(f"{alias!r} resolves to no physical database — bootstrap the group first")
    prefix = layout.physical_prefix(group_id, alias, physical)
    if group is not None:  # not backed up by design (#112): read-only, or writes disposable
        decision = backs_up(neo4j, group, physical)
        if not decision["backs_up"]:
            log(f"{group_id}/{alias}: not backed up by design ({decision['reason']})")
            return {"group": group_id, "alias": alias, "physical": physical, "kind": kind,
                    "artifact": None, "taken": None, "skipped": True, "needs_full": False,
                    "by_design": decision["reason"]}
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
    store = _store(store, group_id)
    head = store.latest_artifact_key(layout.alias_prefix(group_id, alias))
    if not head:
        raise OpError(f"no artifact for {group_id}/{alias}")
    return head, layout.physical_of_key(group_id, alias, head)


def aggregate_target(run_admin: RunAdmin, store, runner, layout, group_id: str, alias: str,
                     *, upload: str = "admin", staging: str | None = None,
                     log: Log = _NOLOG, skip_missing: bool = False) -> dict:
    """Collapse the live physical's chain into one recovered full, IN PLACE (RTO/retention).
    `skip_missing` (scheduled runs, #104): a member with no backups yet is skipped, not failed."""
    runner = _per(runner, group_id)
    upload = _upload(runner, upload)
    store = _store(store, group_id)
    if skip_missing and not store.latest_artifact_key(layout.alias_prefix(group_id, alias)):
        log(f"{group_id}/{alias}: no backups yet; aggregate skipped")
        return {"alias": alias, "physical": None, "full": None, "skipped": True}
    _head, physical = head_physical(store, layout, group_id, alias)
    prefix = layout.physical_prefix(group_id, alias, physical)
    full = run_aggregate(run_admin, runner, store, physical, prefix, upload=upload, staging=staging,
                         log=log)
    return {"alias": alias, "physical": physical, "full": full}


def verify_target(run_admin: RunAdmin, store, runner, layout, group_id: str, alias: str,
                  *, upload: str = "admin", staging: str | None = None,
                  skip_missing: bool = False, log: Log = _NOLOG) -> dict:
    """Non-destructive consistency check of the latest backup for the alias. `skip_missing`
    (scheduled runs, #104): a member with no backups yet is skipped, not failed."""
    runner = _per(runner, group_id)
    upload = _upload(runner, upload)
    store = _store(store, group_id)
    if skip_missing and not store.latest_artifact_key(layout.alias_prefix(group_id, alias)):
        log(f"{group_id}/{alias}: no backups yet; verify skipped")
        return {"alias": alias, "physical": None, "consistent": None, "skipped": True}
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
    runner = _per(runner, group_id) if runner is not None else None
    store = _store(store, group_id)
    prefix = layout.alias_prefix(group_id, alias)
    if run_admin is not None and runner is not None:
        _metas, unknown = artifacts.read(store, prefix)
        for folder in sorted({k.rsplit("/", 1)[0] + "/" for k in unknown}):
            record_artifacts(run_admin, runner, store, folder, log=log)
    return artifacts.chains(store, prefix)


def load_artifacts(store, layout, group_id: str, alias: str) -> list[dict]:
    """A member's load artifacts (#156), oldest first: {key, kind, modified}."""
    store = _store(store, group_id)
    lp = layout.load_prefix(group_id, alias)
    return [{"key": k, "kind": kind, "modified": m.isoformat() if hasattr(m, "isoformat") else m}
            for (k, m, kind) in store.list_load_artifacts(lp)]


def import_to_dump(run_admin: RunAdmin, runner, store, layout, group_id: str, member: str,
                   database: str, source_args: list, *, upload: str = "admin",
                   staging: str | None = None, log: Log = _NOLOG) -> dict:
    """Import on the loader, dump the offline store and put the dump in the member's load folder
    (#158): `<group>/<slug>/_seed/<ts>/<database>.dump`. The loader never starts a server, so
    the import can't be quarantined (IMPORT.md). `upload=pipeline` dumps to the runner's local
    staging and uploads through the store (SSE headers); otherwise neo4j-admin writes directly."""
    store, runner = _store(store, group_id), _per(runner, group_id)
    upload = _upload(runner, upload)
    prefix = f"{layout.load_prefix(group_id, member)}{naming.ts()}/"
    # import, dump and remove the store in one runner step (#167)
    if upload == "pipeline":
        stage = _stage(runner, "_dump", database, staging)
        os.makedirs(stage, exist_ok=True)
        cmd = runner.import_dump_command(database, source_args, stage)
        _admin(run_admin, cmd, store)
        key = store.upload_file(os.path.join(stage, f"{database}.dump"),
                                f"{prefix}{database}.dump")
        shutil.rmtree(stage, ignore_errors=True)
    else:
        cmd = runner.import_dump_command(database, source_args, store.uri(prefix))
        _admin(run_admin, cmd, store)
        key = f"{prefix}{database}.dump"
    log(f"imported {database} and dumped it to {key}")
    return {"database": database, "group": group_id, "member": member, "key": key,
            "uri": store.uri(key), "argv": cmd}


def export_dumps(run_admin: RunAdmin, runner, store, layout, group, *, until: str | None = None,
                 log: Log = _NOLOG) -> dict:
    """A portable `.dump` of every member at `until` (or its latest backup) (#160), written to the
    member's `_seed/<ts>/` folder. Each member's chain is chosen as a restore would (#114); the
    runner restores it into a throwaway local store, dumps it, and removes the store, so the
    cluster is never touched. A member whose seed is already a load artifact is reported, not
    copied."""
    store, runner = _store(store, group.id), _per(runner, group.id)
    seeds = choose_seeds(store, layout, group, until)
    ts = naming.ts()
    out = []
    for name in group.names:
        seed = seeds[name]
        if seed.get("load"):
            out.append({"member": name, "key": seed["key"], "already_a_dump": True})
            continue
        local = f"export-{naming.slug(name)}-{ts}"[:63]
        prefix = f"{layout.load_prefix(group.id, name)}{ts}/"
        cmd = runner.export_command(
            store.uri(seed["key"]), local, store.uri(prefix),
            until=_admin_until(seed["until"]))
        _admin(run_admin, cmd, store)
        key = f"{prefix}{local}.dump"
        log(f"exported {group.id}/{name} -> {key}")
        out.append({"member": name, "key": key, "from": seed["key"], "until": seed["until"],
                    "chain": seed.get("chain")})
    return {"group": group.id, "until": until, "exports": out}


def _admin_until(until: str | None) -> str | None:
    """neo4j-admin's --restore-until takes `YYYY-MM-DD HH:MM:SS` (UTC); finer precision is
    dropped, which can only exclude transactions from that last second."""
    if not until:
        return None
    return _sidecar_time(until).strftime("%Y-%m-%d %H:%M:%S")


def import_database(run_admin: RunAdmin, runner, database: str, source_args: list) -> dict:
    """Bulk-import raw data into an offline store on the loader (#16): the reusable tail's first step.
    Structures + runs `neo4j-admin database import full`; `source_args` is a passthrough. The rest of
    the tail (start Neo4j → CREATE DATABASE → backup → verify → seed) is existing commands — see
    IMPORT.md. No store/neo4j handle: import is local to the loader, not a network/object-store op."""
    cmd = runner.import_command(database, source_args)
    run_admin(cmd)
    return {"database": database, "argv": cmd}


def _cluster(handle, cluster: str):
    """A handle for one cluster (#106): a Neo4jSet/RunnerSet's `named`, a StoreSet's
    `for_cluster`; a plain handle as is."""
    if hasattr(handle, "for_cluster"):
        return handle.for_cluster(cluster)
    return handle.named(cluster) if hasattr(handle, "named") else handle


def system_backup(run_admin: RunAdmin, store, runner, layout,
                  *, upload: str = "admin", staging: str | None = None,
                  cluster: str = "default") -> dict:
    """Binary FULL backup of a cluster's `system` database to its `system_prefix` (#15, #106)."""
    store, runner = _cluster(store, cluster), _cluster(runner, cluster)
    upload = _upload(runner, upload)
    prefix = layout.system_prefix(cluster)
    key = run_backup(run_admin, runner, store, "system", prefix, "FULL",
                     upload=upload, staging=staging)
    return {"key": key, "cluster": cluster}


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


def prune(store, policy, layout, *, keep_system: int | None = None,
          keep_metadata: int | None = None, dry_run: bool = False,
          run_admin: RunAdmin | None = None, runner=None, now: datetime | None = None,
          log: Log = _NOLOG, groups: list | None = None, dbms: bool = True,
          cluster: str | None = None) -> dict:
    """Delete backups past each group's `retention_days` without breaking a chain (#73): per
    member, whole chains are deleted only when nothing kept depends on them
    (`plan_member_prune`). Full or diff is read from the sidecars (#118). Artifacts with no sidecar
    are inspected first when `run_admin`/`runner` are given (this writes sidecars even on a dry
    run, so the preview and the real prune see the same chains); otherwise they are kept and
    counted in `unknown_kept`. Also keeps the newest `keep_metadata` DBMS metadata exports and
    `keep_system` system fulls (both integers >= 1, #78; default: the policy's `dbms:` block).
    `dry_run` lists the victims (`keys`) without deleting. `groups` limits the artifact prune to
    those groups (a tier's, #104); `dbms=False` skips the DBMS sweep; `cluster` limits the sweep
    to one cluster (default: every cluster, #106)."""
    pd = getattr(policy, "dbms", None)
    keep_system = keep_system if keep_system is not None else getattr(pd, "keep_system", 14)
    keep_metadata = (keep_metadata if keep_metadata is not None
                     else getattr(pd, "keep_metadata", 14))
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
    base = store
    loads_victims: list[str] = []
    retired_out: list[dict] = []
    for g in (policy.db_groups if groups is None else groups):
        store = _store(base, g.id)  # each group's own store (#105)
        if getattr(g, "retired", False):  # offboarding (#124): all or nothing, by date
            ends = g.deletes_on()
            if now.date() < ends:
                retired_out.append({"group": g.id, "deletes_on": ends.isoformat(),
                                    "deleted": False})
                continue
            prefixes = [layout.alias_prefix(g.id, a) for a in g.names]
            keys = [k for p in prefixes for (k, _s, _m) in store.list_artifacts(p)]
            loads = [k for a in g.names for (k, _m, _kind) in
                     store.list_load_artifacts(layout.load_prefix(g.id, a))]
            victims.extend(keys + loads)
            detail[f"{g.id}/retired"] = len(keys) + len(loads)
            if not dry_run:
                for p in prefixes:
                    store.delete_prefix(p)
            retired_out.append({"group": g.id, "deletes_on": ends.isoformat(),
                                "deleted": not dry_run, "prefixes": prefixes})
            log(f"{g.id}: retired since {g.retired_on}; "
                + ("would delete" if dry_run else "deleted") + f" {', '.join(prefixes)}")
            continue
        cutoff = now - timedelta(days=g.retention_days)
        keep_loads = g.load.keep_loads if hasattr(g, "load") else 14
        for a in g.names:
            # load artifacts (#157): the newest `keep_loads` per member, not age-swept
            if hasattr(store, "list_load_artifacts"):
                loads = store.list_load_artifacts(layout.load_prefix(g.id, a))
                old = [k for (k, _m, _kind) in loads[:-keep_loads]]
                loads_victims.extend(old)
                _sweep(old, f"{g.id}/{a}/loads")
            prefix = layout.alias_prefix(g.id, a)
            if not store.list_artifacts(prefix):
                continue
            read = artifact_chains(store, layout, g.id, a, run_admin=run_admin, runner=runner,
                                   log=log)
            keys, held = plan_member_prune(read["chains"], cutoff)
            held_total += held
            unknown_kept.extend(read["unknown"])
            _sweep(keys, f"{g.id}/{a}")
            swept.append((store, prefix))
    # DBMS objects: each cluster's, in that cluster's store (#106); exact-prefix listings
    clusters = ([] if not dbms else [cluster] if cluster
                else (policy.cluster_names() if hasattr(policy, "cluster_names") else ["default"]))
    for cl in clusters:
        store = _cluster(base, cl)
        tag = "_dbms" if cl == "default" else f"_dbms/{cl}"
        marts = sorted(store.list_text_keys(layout.metadata_listing(cl)), key=lambda t: t[1])
        _sweep([k for (k, _m) in marts[:-keep_metadata]], f"{tag}/metadata")
        sysarts = sorted(store.list_artifacts(layout.system_prefix(cl)), key=lambda t: t[2])
        _sweep([k for (k, _s, _m) in sysarts[:-keep_system]], f"{tag}/system")
        swept.append((store, layout.system_prefix(cl)))
    # sidecars (#118) go with their artifacts
    sidecars = 0
    if not dry_run:
        for st, prefix in swept:
            sidecars += len(artifacts.drop_orphans(st, prefix))
    return {"deleted": len(victims), "detail": detail, "keys": victims, "dry_run": dry_run,
            "sidecars_deleted": sidecars, "kept_for_chains": held_total,
            "unknown_kept": len(unknown_kept), "loads": loads_victims,
            "retired": retired_out}


# --- DBMS metadata (pure Bolt, no runner) ------------------------------------------------------

def export_metadata(neo4j, store, layout, cluster: str = "default") -> dict:
    """Capture a cluster's security + alias layer as replayable Cypher under its `_dbms/` prefix
    (#14, #106)."""
    neo4j, store = _cluster(neo4j, cluster), _cluster(store, cluster)
    ts = naming.ts()
    key = layout.metadata_key(ts, cluster)
    cypher = metadata.render(metadata.capture(neo4j), ts=ts)
    store.put_text(key, cypher)
    return {"key": key, "bytes": len(cypher), "cluster": cluster}


def restore_metadata(neo4j, store, layout, key: str | None = None,
                     cluster: str = "default") -> dict:
    """Replay a cluster's metadata export (latest, or a given `key`) against its `system`."""
    neo4j, store = _cluster(neo4j, cluster), _cluster(store, cluster)
    key = key or store.latest_text_key(layout.metadata_listing(cluster))
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


def _from_load(store, layout, group_id: str, name: str, until, load) -> dict | None:
    """The load-artifact seed (#144, #159), or None when the backup chains apply. `load` is
    None, "latest" (the member's newest load), or a key in its `_seed/` folder or a URI a server
    can read (an external dump). A load restores whole, so `until` is refused with one; a member
    with no backups at all and no `until` falls back to its newest load."""
    loads = store.list_load_artifacts(layout.load_prefix(group_id, name)) \
        if hasattr(store, "list_load_artifacts") else []
    if load is None:
        if until is not None or store.latest_artifact_key(layout.alias_prefix(group_id, name)):
            return None
        if not loads:
            return None
        load = "latest"
    if until is not None:
        raise OpError(f"{group_id}/{name}: a load artifact restores whole; drop --until, or "
                      f"restore from the backup chains instead")
    if load == "latest":
        if not loads:
            raise OpError(f"no load artifact for {group_id}/{name}")
        key = loads[-1][0]
        return {"key": key, "uri": store.uri(key), "until": None, "chain": None, "whole": True,
                "load": True}
    if "://" in load or load.startswith("file:"):  # an external dump (inbound seeding, #159)
        return {"key": load, "uri": load, "until": None, "chain": None, "whole": True,
                "load": True, "external": True}
    if load not in {k for (k, _m, _kind) in loads}:
        raise OpError(f"{load!r} is not a load artifact of {group_id}/{name} "
                      f"(under {layout.load_prefix(group_id, name)})")
    return {"key": load, "uri": store.uri(load), "until": None, "chain": None, "whole": True,
            "load": True}


def choose_seed(store, layout, group_id: str, name: str, until: str | None = None,
                load: str | None = None) -> dict:
    """The artifact to seed one member from (#114), and the `seedRestoreUntil` to use with it.

    Without `until`: the member's newest artifact, as before. With `until` T (normalised, #77),
    from the chains in the sidecars (#118), across every physical and store of the member:
    - T inside a chain's window (its full's time <= T < its newest backup's time): that chain's
      newest artifact, with `seedRestoreUntil` T. This reaches a time before a restore or rebuild,
      which lives in the previous physical's chain.
    - T at a chain's newest backup, or after the member's newest backup overall: that artifact,
      whole (no `seedRestoreUntil`; Neo4j refuses it on a full, and nothing later was backed up).
    - Otherwise (before the first backup, or in a gap between chains): OpError listing the
      nearest windows, raised before anything is changed.
    Load artifacts (#144): `load="latest"` or a key/URI seeds from a load; `--until` never picks
    one; a member with no backups falls back to its newest load.
    Returns {key, uri, until, chain: {folder, from, to} | None, whole: bool, load?: bool}."""
    store = _store(store, group_id)
    picked = _from_load(store, layout, group_id, name, until, load)
    if picked is not None:
        return picked
    prefix = layout.alias_prefix(group_id, name)
    if until is None:
        key = store.latest_artifact_key(prefix)
        if not key:
            raise OpError(f"no artifact for {group_id}/{name} — back up first")
        return {"key": key, "until": None, "chain": None, "whole": True}
    from .until import parse_until
    t = _sidecar_time(parse_until(until))
    read = artifacts.chains(store, prefix)
    chains = sorted(read["chains"], key=lambda c: c["from"] or "")
    if not chains:
        raise OpError(f"no backups with metadata for {group_id}/{name}"
                      + (f" ({len(read['unknown'])} without a sidecar: run a backup or "
                         f"`show_chain` to fill them in)" if read["unknown"] else ""))
    win = lambda c: {"folder": c["folder"], "from": c["from"], "to": c["to"]}  # noqa: E731
    newest = max(chains, key=lambda c: c["to"] or "")
    for c in chains:
        start, end = _sidecar_time(c["from"]), _sidecar_time(c["to"])
        # a diff with no transactions can't take seedRestoreUntil (Neo4j treats it as a full,
        # verified on 2026.05), so the seed is the newest diff that has transactions; with
        # none, the chain is its full and restores whole
        real = [a for a in c["artifacts"] if a["type"] == "full" or a.get("highest_tx")]
        last = real[-1] if real else c["artifacts"][0]
        if start <= t < end:
            if last["type"] == "full" or _sidecar_time(last["time"]) <= t:
                return {"key": last["key"], "until": None, "chain": win(c), "whole": True}
            return {"key": last["key"], "until": parse_until(until), "chain": win(c),
                    "whole": False}
        if t == end or (c is newest and t >= end):
            return {"key": last["key"], "until": None, "chain": win(c), "whole": True}
    near = sorted(chains, key=lambda c: min(abs((_sidecar_time(c["from"]) - t).total_seconds()),
                                            abs((_sidecar_time(c["to"]) - t).total_seconds())))
    windows = ", ".join(f"{c['from']}..{c['to']} ({c['folder']})" for c in near[:3])
    unknown = (f"; {len(read['unknown'])} backups have no sidecar and were not considered"
               if read["unknown"] else "")
    raise OpError(f"no backup chain of {group_id}/{name} covers {until}; nearest windows (UTC): "
                  f"{windows}{unknown}")


def choose_seeds(store, layout, group, until: str | None = None,
                 loads: dict | str | None = None) -> dict[str, dict]:
    """`choose_seed` for every member, so a time no chain covers (or a missing load) fails before
    any change. `loads`: "latest" for every member, or {member: key-or-uri}."""
    if isinstance(loads, dict):
        unknown = set(loads) - set(group.names)
        if unknown:
            raise OpError(f"--from-dump names {sorted(unknown)}, not members of {group.id}")
    pick = (lambda n: loads.get(n)) if isinstance(loads, dict) else (lambda n: loads)
    return {name: choose_seed(store, layout, group.id, name, until, pick(name))
            for name in group.names}


class _Members:
    """A group seen through a subset of its members (#121): every attribute as the group's, but
    `names` (and so the restore, privileges, baseline and chain choice) only the chosen ones."""

    def __init__(self, group, names: list[str]):
        self._group, self.names = group, names

    def __getattr__(self, attr):
        return getattr(self._group, attr)


def only_members(group, members: list[str] | None):
    """`group` limited to `members` (a restore of some members, #121), or the group itself."""
    if not members:
        return group
    unknown = [m for m in members if m not in group.names]
    if unknown:
        raise OpError(f"{unknown} are not members of {group.id} (members: {group.names})")
    return _Members(group, [n for n in group.names if n in members])


def _restore_by_name(neo4j, group, name: str, key: str, existed: bool, *, store,
                     restore_until, cypher_version, log: Log, uri: str | None = None) -> dict:
    """DROP (if present) then CREATE under the same name. DROP removes the database's privileges,
    so they are captured first and re-granted after the create (#117)."""
    held = _privileges_of(neo4j, name) if existed else []
    if existed:
        neo4j.drop_database(name)
        log(f"dropped {name} (replace)")
    neo4j.seed_database(name, uri or store.uri(key), restore_until=restore_until,
                        topology=_topology(neo4j, group, name), cypher_version=cypher_version,
                        store_base=None if uri and not uri.startswith(store.uri(""))
                        else store.uri(""))
    log(f"restored {name} <= {key}")
    apply_access(neo4j, group, name, log=log)  # writes: read-only (#112)
    try:
        granted = regrant(neo4j, held, log=log)
    except OpError as e:
        raise OpError(f"{name} restored from {key}, but its privileges were not all re-granted: "
                      f"{e}") from e
    return {"mode": "by-name", "name": name, "key": key, "replaced": existed,
            "privileges": granted}


def seed_member(neo4j, store, group, layout, name: str, *, restore_until: str | None = None,
                replace: bool = False, cypher_version: str | None = None, ts: str | None = None,
                log: Log = _NOLOG, seed: dict | None = None) -> dict:
    """Seed one group member from its latest artifact. by-name: create-if-absent, or DROP+recreate
    an existing target with `replace` (destructive; Neo4j has no rename), keeping its privileges.
    alias-swap: seed a fresh physical named for `ts` (one group-wide timestamp; defaults to now) —
    the caller cuts the alias over afterwards (`cutover_seeded`, which copies privileges first).
    Returns a plan entry tagged with `mode`. The artifact (and whether `restore_until` applies)
    comes from `choose_seed` (#114), or `seed` when the caller chose already."""
    neo4j = _per(neo4j, group.id)
    cypher_version = _cypher(neo4j, cypher_version)
    store = _store(store, group.id)
    seed = seed or choose_seed(store, layout, group.id, name, restore_until)
    key, until = seed["key"], seed["until"]
    if group.restore_mode == "by-name":
        existed = neo4j.database_exists(name)
        if existed and not replace:
            raise OpError(f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
        out = _restore_by_name(neo4j, group, name, key, existed, store=store,
                               restore_until=until, cypher_version=cypher_version, log=log,
                               uri=seed.get("uri"))
        return {**out, "seed": seed}
    old = neo4j.alias_target(name)  # captured before cutover (for external routing #17)
    newdb = naming.scheme().physical(name, ts or naming.ts(), old)  # (#89)
    if newdb == old or (hasattr(neo4j, "database_exists") and neo4j.database_exists(newdb)):
        # a name-reusing scheme (blue/green) must not seed over a database in use (#89)
        raise OpError(f"{group.id}/{name}: the new physical name {newdb!r} is already taken"
                      + (" (it's the alias's current target)" if newdb == old else ""))
    uri = seed.get("uri") or store.uri(key)
    neo4j.seed_database(newdb, uri, restore_until=until,
                        topology=_topology(neo4j, group, name), cypher_version=cypher_version,
                        # an external dump (#159) is outside the backup store by design
                        store_base=None if seed.get("external") else store.uri(""))
    log(f"seeded {newdb} <= {key}" + (f" until {until}" if until else ""))
    apply_access(neo4j, group, newdb, log=log)  # writes: read-only, before the swap (#112)
    return {"mode": "alias-swap", "alias": name, "newdb": newdb, "old": old, "key": key,
            "seed": seed, "group": group.id}


def check_seeded(neo4j, group, seeded: list[dict], *, expect_counts: dict | None = None,
                 keep_failed: bool = False, require_data: bool = True,
                 log: Log = _NOLOG) -> None:
    """Run the default and `load.checks` checks on every newly seeded physical (#111); results go
    into each member entry under `checks`. A failure drops every new physical (unless
    `keep_failed`) and raises OpError naming the check and value; no alias has moved."""
    from . import checks

    pol_checks = list(getattr(getattr(group, "load", None), "checks", []) or [])
    for s in seeded:
        db = s.get("newdb") or s.get("name")
        try:
            s["checks"] = checks.run(neo4j, db, pol_checks, require_data=require_data,
                                     expect_counts=(expect_counts or {}).get(s.get("alias")
                                                                             or s.get("name")))
            log(f"checks passed on {db}: {len(s['checks'])}")
        except (checks.CheckFailed, ValueError) as e:
            if not keep_failed:
                for t in seeded:
                    if t.get("newdb"):
                        neo4j.drop_database(t["newdb"])
                log(f"dropped the new databases ({e})")
            raise OpError(f"{e}; no alias was moved"
                          + ("" if keep_failed else "; the new databases were dropped")) from e


def plan_cleanup(neo4j, group, *, keep: int = 1, older_than_days: float | None = None,
                 now: datetime | None = None) -> dict:
    """Superseded physicals a group can drop (#93). A candidate is a database the naming scheme
    says the tool created for the member (`owns`, #89), that no alias targets, and that is
    neither the current target nor one of the `keep` newest before it (rollback, #122). With
    `older_than_days`, only databases created longer ago. Databases a customer created are never
    candidates."""
    keep_count("keep", keep)
    neo4j = _per(neo4j, group.id)
    now = now or datetime.now(timezone.utc)
    targeted = set(neo4j.list_aliases().values())
    all_dbs = sorted(set(neo4j.list_databases()))  # one row per server on a cluster
    members = []
    for name in group.names:
        if group.restore_mode == "by-name":
            continue  # restored in place: no superseded physicals
        current = neo4j.alias_target(name)
        mine = [d for d in all_dbs if naming.scheme().owns(name, d) and d != current]
        created = {d: neo4j.database_created(d) for d in mine}
        newest_first = sorted(mine, key=lambda d: created[d] or now, reverse=True)
        kept = newest_first[:keep]
        drops = []
        for d in newest_first[keep:]:
            if d in targeted:
                continue
            c = created[d]
            if older_than_days is not None and c is not None and \
                    (now - _floor(c)).total_seconds() < older_than_days * 86400:
                continue
            drops.append(d)
        members.append({"alias": name, "current": current, "kept": kept, "drops": drops})
    return {"group": group.id, "members": members,
            "drops": [d for m in members for d in m["drops"]]}


def cleanup(neo4j, group, *, keep: int = 1, older_than_days: float | None = None,
            log: Log = _NOLOG) -> dict:
    """Drop the databases `plan_cleanup` lists, first revoking the privileges copied to them
    (#117), so no grant naming a dropped database is left behind."""
    from .privileges import revoke

    plan = plan_cleanup(neo4j, group, keep=keep, older_than_days=older_than_days)
    neo = _per(neo4j, group.id)
    for d in plan["drops"]:
        for cmd in neo.privilege_commands(d):
            neo.run_system(revoke(cmd))
        neo.drop_database(d)
        log(f"dropped {d}")
    return plan


STATE_PREFIX = "_state/"


def record_swaps(store, group_id: str, swaps: list[dict], *, keep: int = 20) -> None:
    """Remember each alias's previous target for `rollback` (#122): `_state/<group>.json` holds the
    last `keep` swaps, newest last. A stand-in until the records layer exists (#100)."""
    import json
    from datetime import datetime, timezone

    key = f"{STATE_PREFIX}{group_id}.json"
    try:
        state = json.loads(store.get_text(key))
    except Exception:  # noqa: BLE001 - no state yet
        state = {"group": group_id, "swaps": []}
    state["swaps"].append({"at": datetime.now(timezone.utc).isoformat(), "members": {
        s["alias"]: {"from": s.get("old"), "to": s["newdb"]} for s in swaps}})
    state["swaps"] = state["swaps"][-keep:]
    store.put_text(key, json.dumps(state, indent=1))


def _state(store, group_id: str) -> dict:
    import json
    try:
        return json.loads(store.get_text(f"{STATE_PREFIX}{group_id}.json"))
    except Exception:  # noqa: BLE001 - no state yet
        return {"group": group_id, "swaps": []}


def fence(store, group_id: str, source: str, target: str) -> None:
    """Mark a group as moved off `source` (#115): its backups, restores and loads that resolve
    to `source` refuse (`check_fence`) until the policy's `cluster:` names another cluster."""
    import json
    from datetime import datetime, timezone

    state = _state(store, group_id)
    state["fenced"] = {"cluster": source, "moved_to": target,
                       "at": datetime.now(timezone.utc).isoformat()}
    store.put_text(f"{STATE_PREFIX}{group_id}.json", json.dumps(state, indent=1))


def check_fence(neo4j, store, group_id: str) -> None:
    """Refuse when the policy puts `group_id` on a cluster it was moved off (#115). Only a
    policy-aware handle (a Neo4jSet) knows the group's cluster; plain handles aren't checked."""
    pol_of = getattr(neo4j, "_pol", None)
    if pol_of is None:
        return
    try:
        pol = pol_of()
        cluster = pol.cluster_name(group_id) if pol is not None else None
    except Exception:  # noqa: BLE001 - not a policy group (a probe)
        return
    if cluster is None:
        return
    fenced = _state(_store(store, group_id), group_id).get("fenced")
    if fenced and fenced.get("cluster") == cluster:
        raise OpError(f"{group_id} was moved from cluster {cluster!r} to "
                      f"{fenced.get('moved_to')!r} at {fenced.get('at')}: set `cluster: "
                      f"{fenced.get('moved_to')}` on the group in the policy (refusing to write "
                      f"to {cluster!r}, so two clusters don't share one chain)")


def last_swap(store, group_id: str) -> dict | None:
    import json
    try:
        swaps = json.loads(store.get_text(f"{STATE_PREFIX}{group_id}.json"))["swaps"]
    except Exception:  # noqa: BLE001
        return None
    return swaps[-1] if swaps else None


def rollback(neo4j, store, group, *, members: list[str] | None = None, dry_run: bool = False,
             log: Log = _NOLOG) -> dict:
    """Point each alias back at the database it targeted before the most recent swap (#122).
    The previous target comes from `_state/<group>.json`. Refuses before any change when it isn't
    known, or no longer exists. Privileges granted on the current database since the swap are
    copied to the previous one first (#117); no baseline (the old chain still exists)."""
    neo4j = _per(neo4j, group.id)
    state_store = _store(store, group.id)  # the group's own store holds its state
    swap = last_swap(state_store, group.id)
    if swap is None:
        raise OpError(f"no recorded swap for {group.id}: nothing to roll back")
    names = members or [n for n in group.names if n in swap["members"]]
    plan = []
    for name in names:
        moved = swap["members"].get(name)
        if not moved or not moved.get("from"):
            raise OpError(f"{group.id}/{name}: the last swap ({swap['at']}) didn't move it from "
                          f"an earlier database")
        current = neo4j.alias_target(name)
        if not neo4j.database_exists(moved["from"]):
            raise OpError(f"{group.id}/{name}: the previous database {moved['from']!r} no longer "
                          f"exists (cleaned up?)")
        plan.append({"alias": name, "newdb": moved["from"], "old": current,
                     "mode": "alias-swap", "group": group.id,
                     "privileges": _privileges_of(neo4j, current)})
    if dry_run:
        return {"group": group.id, "swap_at": swap["at"], "dry_run": True, "members": plan}
    cutover_seeded(neo4j, plan, log=log, store=state_store, group_id=group.id)
    return {"group": group.id, "swap_at": swap["at"], "members": plan}


def cutover_seeded(neo4j, seeded: list[dict], *, log: Log = _NOLOG, store=None,
                   group_id: str | None = None) -> None:
    """Barrier after alias-swap seeds. First copy every member's privileges from its old physical
    to its new one (#117); only when all copies succeed, point each alias at its new physical. A
    failed copy raises before any alias moves. Privileges on the old physical are left in place
    (a rollback needs them; DROP DATABASE removes them). by-name entries are no-ops here."""
    swaps = [s for s in seeded if s.get("mode") != "by-name"]
    for s in swaps:
        neo = _per(neo4j, s["group"]) if s.get("group") else neo4j  # its cluster (#106)
        try:
            s["privileges"] = regrant(neo, _privileges_of(neo, s.get("old")),
                                      old=s.get("old"), new=s["newdb"], log=log)
        except OpError as e:
            raise OpError(f"{s['alias']}: copying privileges {s.get('old')} -> {s['newdb']} "
                          f"failed, no alias was moved: {e}") from e
    for s in swaps:
        neo = _per(neo4j, s["group"]) if s.get("group") else neo4j
        # alias-swap (default) or external routing (#17); a cluster may set its own (#106)
        strategy = cutover.for_cluster(getattr(neo, "cutover_config", None))
        strategy.cutover(neo, s["alias"], s["newdb"], s.get("old"))
        log(f"cutover {s['alias']}: {s.get('old')} -> {s['newdb']}")
    gid = group_id or next((s["group"] for s in swaps if s.get("group")), None)
    if store is not None and gid and swaps:  # for rollback (#122)
        try:
            record_swaps(_store(store, gid), gid, swaps)
        except Exception as e:  # noqa: BLE001 - the swap is done; only the record failed
            log(f"warning: could not record the swap for rollback: {e}")


def restore_group(neo4j, store, group, layout, *, restore_until: str | None = None,
                  replace: bool = False, cypher_version: str | None = None, log: Log = _NOLOG,
                  loads: dict | str | None = None, check: bool = False,
                  keep_failed: bool = False) -> dict:
    """Restore a whole group in one process (used by the CLI and Dagster; Airflow maps `seed_member`
    across tasks instead). by-name pre-validates every artifact + existing-target precondition BEFORE
    any drop, so a missing artifact or an unreplaceable target fails before anything is destroyed.
    alias-swap seeds every fresh physical, then cuts all aliases over. Returns the member plan."""
    check_fence(neo4j, store, group.id)  # moved to another cluster (#115)
    neo4j = _per(neo4j, group.id)
    cypher_version = _cypher(neo4j, cypher_version)
    store = _store(store, group.id)
    members: list[dict] = []
    seeds = choose_seeds(store, layout, group, restore_until, loads)  # all, before any change
    if group.restore_mode == "by-name":
        plan: list[tuple[str, bool]] = []
        for name in group.names:
            existed = neo4j.database_exists(name)
            if existed and not replace:
                raise OpError(
                    f"database {name!r} exists; set replace=true to DROP+recreate it (destructive)")
            plan.append((name, existed))
        for name, existed in plan:
            out = _restore_by_name(neo4j, group, name, seeds[name]["key"], existed, store=store,
                                   restore_until=seeds[name]["until"],
                                   cypher_version=cypher_version, log=log,
                                   uri=seeds[name].get("uri"))
            members.append({**out, "seed": seeds[name]})
        return {"mode": "by-name", "members": members}
    ts = naming.ts()  # one timestamp for every physical seeded in this group restore
    members = seed_all(neo4j, group, group.names, lambda name: seed_member(
        neo4j, store, group, layout, name, restore_until=restore_until,
        cypher_version=cypher_version, ts=ts, log=log, seed=seeds[name]), log=log)
    if check:  # `restore --check` (#111): the policy's checks before any alias moves
        check_seeded(neo4j, group, members, keep_failed=keep_failed, require_data=False, log=log)
    cutover_seeded(neo4j, members, log=log, store=store, group_id=group.id)
    return {"mode": "alias-swap", "members": members}


def baseline_after_restore(run_admin: RunAdmin, neo4j, store, runner, layout, group,
                           members: list[dict], *, upload: str = "admin",
                           staging: str | None = None, log: Log = _NOLOG) -> list[dict]:
    """A FULL backup of every database a restore created (#97), so the next diff has a base
    (#74) and point-in-time coverage of the new database starts now rather than at the next full
    schedule. alias-swap members are backed up by their new physical; by-name members by name.
    Raises BaselineError (the restore stays applied) at the first failure."""
    neo4j, runner = _per(neo4j, group.id), _per(runner, group.id)
    done: list[dict] = []
    for m in members:
        alias = m.get("alias") or m["name"]
        physical = m.get("newdb") or m["name"]
        try:
            out = backup_target(run_admin, neo4j, store, runner, layout, group.id, alias, "FULL",
                                upload=upload, staging=staging, log=log, physical=physical,
                                group=group)
        except Exception as e:  # noqa: BLE001 - any failure: the restore itself is done
            raise BaselineError(
                f"restore applied, but the baseline full of {physical} failed ({e}); run "
                f"`neo4j-backup backup {group.id} --kind FULL`", done) from e
        done.append({"member": alias, "physical": physical, "artifact": out["artifact"],
                     "taken": out["taken"]})
        log(f"baseline {physical} -> {out['artifact']}")
    return done


def plan_restore(neo4j, store, group, layout, *, replace: bool = False,
                 until: str | None = None, loads: dict | str | None = None) -> dict:
    """The blast radius of `restore_group` WITHOUT mutating: per member, the artifact that would be
    used and what would happen — by-name `create` or `drop+recreate`, alias-swap seed + swap. Runs
    the same preconditions (missing artifact, unreplaceable existing target) so a dry-run fails the
    same way a real restore would. `drops` is the destructive surface (databases to be DROPped).
    With `until`, each member's `seed` shows the chosen chain and window (#114). Restoring only
    some members (#121) adds a `warning`: they are no longer aligned in time with the rest."""
    neo4j = _per(neo4j, group.id)
    members: list[dict] = []
    seeds = choose_seeds(store, layout, group, until, loads)
    for name in group.names:
        key = seeds[name]["key"]
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
    for m in members:
        m["seed"] = seeds[m.get("alias") or m["name"]]
    out = {"mode": group.restore_mode, "members": members,
           "drops": [d for m in members for d in m["drops"]]}
    if isinstance(group, _Members) and len(group.names) < len(group._group.names):
        rest = [n for n in group._group.names if n not in group.names]
        out["warning"] = (f"restoring {group.names} only: they will no longer be aligned in time "
                          f"with {rest}, the rest of group {group.id}")
    return out
