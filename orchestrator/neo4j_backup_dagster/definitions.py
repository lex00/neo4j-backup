"""The backup code location's single Definitions (DESIGN.md §6).

Storage layout is per physical store: `<group>/<slug>/<physical>/<artifact>.backup`.
A backup chain lives in one `<physical>/` directory (one store = a valid chain); the
`<slug>/` level groups all of an alias's physicals so "latest backup for the alias" is
the newest artifact across them. This is the Phase 5 fix for chain-mixing.
"""

import os

import dagster as dg

from neo4j_backup_core import cutover, metadata, naming, paths
from neo4j_backup_core.policy import load_policy, parse_partition_key
from .resources import Neo4jResource, ObjectStoreResource, RunnerResource

POLICY_PATH = os.environ.get("NEO4J_BACKUP_POLICY", "policies/demo.yaml")
# Cypher language for the seed statement: unset (Cypher-25 default, omit existingData) or "5"
# on a Cypher-5 cluster (existingData required). See Neo4jClient.seed_database.
SEED_CYPHER_VERSION = os.environ.get("SEED_CYPHER_VERSION") or None

# Backup upload path: "admin" (default) lets neo4j-admin write straight to s3:// (--to-path).
# "pipeline" makes neo4j-admin write to a local staging dir and the pipeline uploads via boto3,
# so an SSE-KMS header is sent — required by buckets that deny header-less PutObject, which
# neo4j-admin cannot satisfy (no such setting exists; Ops Manual). Subprocess mode only.
BACKUP_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
UPLOAD_STAGING_PATH = os.environ.get("UPLOAD_STAGING_PATH") or None

targets = dg.DynamicPartitionsDefinition(name="backup_targets")


def _run_admin(context, runner, command, subprocess_client, env=None):
    """Run a neo4j-admin command via Pipes — subprocess (VM/EC2, validated) or a k8s
    pod, per `runner.mode`. Non-zero exit raises; neo4j-admin is not Pipes-instrumented
    so callers emit their own result.

    `dagster-k8s` is imported only when RUNNER_MODE=k8s, so a subprocess-only (EC2/VM)
    deployment doesn't need it installed — `pip install 'neo4j-backup-dagster[k8s]'` adds it."""
    env = env or {}
    if runner.mode == "k8s":
        if not runner.image:
            raise dg.Failure("runner.mode='k8s' requires runner.image")
        try:
            from dagster_k8s import PipesK8sClient
        except ImportError as e:
            raise dg.Failure(
                "RUNNER_MODE=k8s needs the 'dagster-k8s' package — "
                "pip install 'neo4j-backup-dagster[k8s]'"
            ) from e
        PipesK8sClient().run(
            context=context, image=runner.image, command=command,
            base_pod_spec=runner.k8s_pod_spec(env),
        )
    else:
        subprocess_client.run(command=command, env=env, context=context)


def _run_backup(context, runner, store, subprocess_client, database, prefix, kind):
    """Run neo4j-admin backup and return the artifact key. admin mode -> neo4j-admin writes
    to s3:// directly. pipeline mode -> neo4j-admin writes to local staging, then the pipeline
    uploads to S3 (SSE-KMS via write_args) for buckets that deny neo4j-admin's header-less
    PutObject. Subprocess mode. (aggregate/verify still write via neo4j-admin — see #39.)"""
    if BACKUP_UPLOAD == "pipeline":
        stage = f"{(UPLOAD_STAGING_PATH or runner.scratch_path).rstrip('/')}/_stage/{database}"
        os.makedirs(stage, exist_ok=True)
        cmd = runner.backup_command(database, stage, kind=kind)
        _run_admin(context, runner, cmd, subprocess_client, env=runner.env())
        return store.upload_backups(stage, prefix)  # SSE-KMS PUT, then remove local
    to_path = store.s3_uri(prefix)
    cmd = runner.backup_command(database, to_path, kind=kind)
    _run_admin(context, runner, cmd, subprocess_client, env=runner.env())
    return store.latest_artifact_key(prefix)


# --- storage-key layout: an injected PathLayout instance (#21), not module aliases. A
# deployment selects a custom scheme with PATH_LAYOUT=module.Class; default is unchanged.
_layout = paths.get_layout()
_alias_prefix = _layout.alias_prefix
_physical_prefix = _layout.physical_prefix
_physical_of_key = _layout.physical_of_key


# --- Backup ----------------------------------------------------------------------
class BackupConfig(dg.Config):
    kind: str = "AUTO"  # AUTO | FULL | DIFF — set by the full/diff schedules


@dg.asset(partitions_def=targets, pool="neo4j", group_name="neo4j_backup")
def backup(
    context: dg.AssetExecutionContext,
    config: BackupConfig,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
    runner: RunnerResource,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    """Back up the physical database the alias currently targets, into that physical's
    own prefix — so repeated backups of the alias form a real chain (same store)."""
    group_id, alias = parse_partition_key(context.partition_key)
    # Accept either an alias (-> its current target) or a physical database name directly.
    physical = neo4j.resolve_physical(alias)
    if not physical:
        raise dg.Failure(f"{alias!r} resolves to no physical database — bootstrap the group first")

    prefix = _physical_prefix(group_id, alias, physical)
    artifact = _run_backup(context, runner, store, pipes_subprocess_client,
                           physical, prefix, config.kind)
    size = store.object_size(artifact) if artifact else 0
    return dg.MaterializeResult(
        metadata={
            "group": group_id, "alias": alias, "physical": physical,
            "kind": config.kind, "artifact": artifact or "",
            "bytes": dg.MetadataValue.int(size), "upload": BACKUP_UPLOAD,
        }
    )


backup_job = dg.define_asset_job(
    "backup_job", selection=[backup], partitions_def=targets
)


# --- Aggregate (retention / RTO, destructive in place) ---------------------------
@dg.asset(partitions_def=targets, group_name="neo4j_backup")
def aggregate(
    context: dg.AssetExecutionContext,
    store: ObjectStoreResource,
    runner: RunnerResource,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    """Collapse the live physical's chain into one recovered full, IN PLACE. RTO lever
    and retention — trades intra-chain PITR, so run on a retention cadence."""
    group_id, alias = parse_partition_key(context.partition_key)
    head = store.latest_artifact_key(_alias_prefix(group_id, alias))
    if not head:
        raise dg.Failure(f"no artifact for {group_id}/{alias}")
    physical = _physical_of_key(group_id, alias, head)
    prefix = _physical_prefix(group_id, alias, physical)
    _run_admin(context, runner, runner.aggregate_command(physical, store.s3_uri(prefix)),
               pipes_subprocess_client, env=runner.env())
    full = store.latest_artifact_key(prefix)
    return dg.MaterializeResult(metadata={"physical": physical, "full": full or ""})


# --- Verify (consistency check, NON-destructive) ---------------------------------
@dg.asset(partitions_def=targets, group_name="neo4j_backup")
def verify(
    context: dg.AssetExecutionContext,
    store: ObjectStoreResource,
    runner: RunnerResource,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    """Prove the latest backup is restorable+consistent without touching the prod chain:
    copy it to a scratch prefix, aggregate the COPY into a recovered full, and
    `database check` it. Non-zero exit raises (fail). Cleans up the copy."""
    group_id, alias = parse_partition_key(context.partition_key)
    head = store.latest_artifact_key(_alias_prefix(group_id, alias))
    if not head:
        raise dg.Failure(f"no artifact for {group_id}/{alias}")
    physical = _physical_of_key(group_id, alias, head)
    src = _physical_prefix(group_id, alias, physical)
    scratch = f"_verify/{group_id}/{physical}/"
    try:
        copied = store.copy_prefix(src, scratch)
        _run_admin(context, runner, runner.aggregate_command(physical, store.s3_uri(scratch)),
                   pipes_subprocess_client, env=runner.env())
        full = store.latest_artifact_key(scratch)
        _run_admin(context, runner, runner.check_command(physical, store.s3_uri(full)),
                   pipes_subprocess_client, env=runner.env())
    finally:
        store.delete_prefix(scratch)
    context.log.info(f"verified {physical}: consistent ({copied} artifacts checked)")
    return dg.MaterializeResult(
        metadata={"alias": alias, "physical": physical, "consistent": True}
    )


# --- Prune (age-based retention) -------------------------------------------------
@dg.asset(group_name="neo4j_backup")
def prune(context: dg.AssetExecutionContext, store: ObjectStoreResource):
    """Delete *.backup older than each group's retention_days, keeping the newest per
    alias (chain head). Production should `aggregate` an old chain into a full first so
    PITR coverage isn't lost; this age prune is chain-naive."""
    from datetime import datetime, timedelta, timezone

    policy = load_policy(POLICY_PATH)
    now = datetime.now(timezone.utc)
    deleted_total = 0
    detail: dict[str, int] = {}
    for g in policy.db_groups:
        cutoff = now - timedelta(days=g.retention_days)
        for a in g.aliases:
            arts = store.list_artifacts(_alias_prefix(g.id, a))
            if not arts:
                continue
            newest = max(arts, key=lambda t: t[2])[0]
            stale = [k for (k, _s, m) in arts if m < cutoff and k != newest]
            n = store.delete_keys(stale)
            deleted_total += n
            if n:
                detail[f"{g.id}/{a}"] = n
    meta_pruned = metadata.prune(store)  # keep the newest N DBMS metadata exports
    deleted_total += meta_pruned
    if meta_pruned:
        detail["_dbms/metadata"] = meta_pruned
    sysarts = sorted(store.list_artifacts(_layout.system_prefix()), key=lambda t: t[2])
    sys_pruned = store.delete_keys([k for (k, _s, _m) in sysarts[:-14]])  # keep newest 14
    deleted_total += sys_pruned
    if sys_pruned:
        detail["_dbms/system"] = sys_pruned
    context.log.info(f"pruned {deleted_total} artifacts")
    return dg.MaterializeResult(
        metadata={"deleted": deleted_total, **{k: dg.MetadataValue.int(v) for k, v in detail.items()}}
    )


# --- System-database binary backup (#15): exact metadata restore (native passwords) ----
@dg.asset(group_name="neo4j_backup")
def system_backup(
    context: dg.AssetExecutionContext,
    store: ObjectStoreResource,
    runner: RunnerResource,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    """Binary backup of the `system` database to the reserved `_dbms/system/` prefix (FULL).
    Restore is offline + node-local (path B) — see bootstrap/restore_system.sh, not a job."""
    prefix = _layout.system_prefix()
    key = _run_backup(context, runner, store, pipes_subprocess_client, "system", prefix, "FULL")
    context.log.info(f"system backup -> {key}")
    return dg.MaterializeResult(metadata={"key": key or ""})


# --- DBMS metadata export (#14): agentless users/roles/privileges/aliases ---------
@dg.asset(group_name="neo4j_backup")
def metadata_export(
    context: dg.AssetExecutionContext,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
) -> dg.MaterializeResult:
    """Capture the DBMS metadata layer as replayable Cypher (pure Bolt, no runner) and
    store it under the reserved `_dbms/` prefix. Restore is `metadata_restore`."""
    ts = naming.ts()
    key = _layout.metadata_key(ts)
    cypher = metadata.render(metadata.capture(neo4j), ts=ts)
    store.put_text(key, cypher)
    context.log.info(f"metadata export -> {key} ({len(cypher)} bytes)")
    return dg.MaterializeResult(metadata={"key": key, "bytes": dg.MetadataValue.int(len(cypher))})


class MetadataRestoreConfig(dg.Config):
    key: str | None = None  # default: the latest _dbms/ artifact


@dg.op
def metadata_restore_op(
    context: dg.OpExecutionContext,
    config: MetadataRestoreConfig,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
):
    key = config.key or store.latest_text_key(_layout.metadata_prefix())
    if not key:
        raise dg.Failure("no metadata artifact — materialize metadata_export first")
    result = metadata.replay(neo4j, store.get_text(key))
    context.log.info(f"replayed {result['applied']} statements from {key}; "
                     f"skipped {len(result['skipped'])}")
    return result


@dg.job
def metadata_restore():
    metadata_restore_op()


# --- Restore (group-aligned alias-swap, pure Cypher) -----------------------------
class RestoreConfig(dg.Config):
    group_id: str
    restore_until: str | None = None  # ISO-8601; needs a differential chain


@dg.op
def restore_group_op(
    context: dg.OpExecutionContext,
    config: RestoreConfig,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
):
    policy = load_policy(POLICY_PATH)
    group = policy.group(config.group_id)
    ts = naming.ts()
    planned: list[tuple[str, str, str | None]] = []
    for alias in group.aliases:
        key = store.latest_artifact_key(_alias_prefix(group.id, alias))
        if not key:
            raise dg.Failure(f"no artifact for {group.id}/{alias} — back up first")
        newdb = naming.physical(alias, ts)
        neo4j.seed_database(newdb, store.s3_uri(key), restore_until=config.restore_until,
                            topology=group.topology_for(alias),
                            cypher_version=SEED_CYPHER_VERSION)
        planned.append((alias, newdb, neo4j.alias_target(alias)))
        context.log.info(f"seeded {newdb} <= {key}")
    strategy = cutover.from_env()  # alias-swap (default) or external routing (#17)
    for alias, newdb, old in planned:
        strategy.cutover(neo4j, alias, newdb, old)
        context.log.info(f"cutover {alias}: {old} -> {newdb}")
    return planned


@dg.job
def restore_group():
    restore_group_op()


# --- Sensor + schedules ----------------------------------------------------------
@dg.sensor(job=backup_job, minimum_interval_seconds=300)
def reconcile_registry(context: dg.SensorEvaluationContext):
    policy = load_policy(POLICY_PATH)
    desired = set(policy.partition_keys())
    existing = set(context.instance.get_dynamic_partitions(targets.name))
    add = sorted(desired - existing)
    remove = sorted(existing - desired)
    reqs = []
    if add:
        reqs.append(targets.build_add_request(add))
    if remove:
        reqs.append(targets.build_delete_request(remove))
    context.log.info(f"reconcile: +{len(add)} -{len(remove)}")
    return dg.SensorResult(dynamic_partitions_requests=reqs)


def _build_schedules() -> list:
    try:
        policy = load_policy(POLICY_PATH)
    except Exception:
        return []
    schedules = []
    for tier_name, tier in policy.tiers.items():
        for lane, cron in (("full", tier.full_cron), ("diff", tier.diff_cron)):
            kind = "FULL" if lane == "full" else "DIFF"

            @dg.schedule(
                job=backup_job, cron_schedule=cron, name=f"{tier_name}_{lane}",
                default_status=dg.DefaultScheduleStatus.STOPPED,
            )
            def _sched(context, _tier=tier_name, _lane=lane, _kind=kind):
                pol = load_policy(POLICY_PATH)
                return [
                    dg.RunRequest(
                        partition_key=f"{g.id}/{a}",
                        tags={"backup_kind": _lane},
                        run_config=dg.RunConfig(ops={"backup": BackupConfig(kind=_kind)}),
                    )
                    for g in pol.groups_for_tier(_tier)
                    for a in g.aliases
                ]

            schedules.append(_sched)
    return schedules


defs = dg.Definitions(
    assets=[backup, aggregate, verify, prune, metadata_export, system_backup],
    jobs=[backup_job, restore_group, metadata_restore],
    schedules=_build_schedules(),
    sensors=[reconcile_registry],
    resources={
        # Credential via a secret provider (#18): default SECRET_PROVIDER=env resolves
        # NEO4J_PASSWORD lazily at connect time (same as before, now rotation-friendly); set
        # SECRET_PROVIDER=aws-sm + NEO4J_PASSWORD_REF=<secret id/ARN> for AWS Secrets Manager.
        "neo4j": Neo4jResource(
            uri=os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"),
            user=os.environ.get("NEO4J_USER", "neo4j"),
            secret_provider=os.environ.get("SECRET_PROVIDER", "env"),
            password_ref=os.environ.get("NEO4J_PASSWORD_REF"),
        ),
        "store": ObjectStoreResource(
            bucket=os.environ.get("BACKUP_BUCKET", "neo4j-backups"),
            # Unset on real AWS S3 (use AWS endpoints); set only for MinIO/S3-compatible.
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3") or None,
            region=os.environ.get("AWS_REGION", "us-east-1"),
            # Explicit encryption header for PUT/COPY (buckets that require it); default: bucket default.
            sse=os.environ.get("S3_SSE") or None,
            sse_kms_key_id=os.environ.get("S3_SSE_KMS_KEY_ID") or None,
            write_args_json=os.environ.get("S3_WRITE_ARGS", "{}"),
        ),
        "runner": RunnerResource(
            backup_source=os.environ.get("NEO4J_BACKUP_SOURCE", "neo4j:6362"),
            scratch_path=os.environ.get("SCRATCH_PATH", "/scratch"),
            pagecache=os.environ.get("RUNNER_PAGECACHE", "512M"),
            heap_size=os.environ.get("RUNNER_HEAP_SIZE", "2G"),
            neo4j_admin=os.environ.get("RUNNER_NEO4J_ADMIN", "neo4j-admin"),
            # Execution mode: "subprocess" (VM/EC2, validated) or "k8s".
            mode=os.environ.get("RUNNER_MODE", "subprocess"),
            image=os.environ.get("RUNNER_IMAGE", ""),
            node_selector_json=os.environ.get("RUNNER_NODE_SELECTOR", "{}"),
            memory_limit=os.environ.get("RUNNER_MEMORY_LIMIT", "4Gi"),
            scratch_storage=os.environ.get("RUNNER_SCRATCH_STORAGE", "6Ti"),
            service_account=os.environ.get("RUNNER_SERVICE_ACCOUNT", ""),
            extra_env_json=os.environ.get("RUNNER_EXTRA_ENV", "{}"),
        ),
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
    },
)
