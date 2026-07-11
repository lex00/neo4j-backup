"""The backup code location's single Definitions (DESIGN.md §6).

Storage layout is per physical store: `<group>/<slug>/<physical>/<artifact>.backup`.
A backup chain lives in one `<physical>/` directory (one store = a valid chain); the
`<slug>/` level groups all of an alias's physicals so "latest backup for the alias" is
the newest artifact across them. This is the Phase 5 fix for chain-mixing.
"""

import os

import dagster as dg

from neo4j_backup_core import ops, paths
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


def _admin(context, runner, subprocess_client):
    """Bind a `run_admin(cmd)` callable for `neo4j_backup_core.ops`: run one neo4j-admin command
    via Pipes (subprocess or k8s) with the runner's environment. The op bodies live in core; this
    is only the Dagster-specific execution + env binding."""
    return lambda cmd: _run_admin(context, runner, cmd, subprocess_client, env=runner.env())


# --- storage-key layout: an injected PathLayout instance (#21), not module aliases. A
# deployment selects a custom scheme with PATH_LAYOUT=module.Class; default is unchanged.
# The ops in `neo4j_backup_core.ops` take this instance and call its prefix methods directly.
_layout = paths.get_layout()


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
    try:
        out = ops.backup_target(
            _admin(context, runner, pipes_subprocess_client), neo4j, store, runner, _layout,
            group_id, alias, config.kind, upload=BACKUP_UPLOAD, staging=UPLOAD_STAGING_PATH,
        )
    except ops.OpError as e:
        raise dg.Failure(str(e))
    artifact = out["artifact"]
    size = store.object_size(artifact) if artifact else 0
    return dg.MaterializeResult(
        metadata={
            "group": group_id, "alias": alias, "physical": out["physical"],
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
    try:
        out = ops.aggregate_target(
            _admin(context, runner, pipes_subprocess_client), store, runner, _layout,
            group_id, alias, upload=BACKUP_UPLOAD, staging=UPLOAD_STAGING_PATH,
        )
    except ops.OpError as e:
        raise dg.Failure(str(e))
    return dg.MaterializeResult(metadata={"physical": out["physical"], "full": out["full"] or ""})


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
    try:
        out = ops.verify_target(
            _admin(context, runner, pipes_subprocess_client), store, runner, _layout,
            group_id, alias, upload=BACKUP_UPLOAD, staging=UPLOAD_STAGING_PATH,
        )
    except ops.OpError as e:
        raise dg.Failure(str(e))
    context.log.info(f"verified {out['physical']}: consistent ({out['checked']} artifacts checked)")
    return dg.MaterializeResult(
        metadata={"alias": alias, "physical": out["physical"], "consistent": True}
    )


# --- Prune (age-based retention) -------------------------------------------------
@dg.asset(group_name="neo4j_backup")
def prune(context: dg.AssetExecutionContext, store: ObjectStoreResource):
    """Delete *.backup older than each group's retention_days, keeping the newest per
    alias (chain head). Production should `aggregate` an old chain into a full first so
    PITR coverage isn't lost; this age prune is chain-naive."""
    out = ops.prune(store, load_policy(POLICY_PATH), _layout)
    context.log.info(f"pruned {out['deleted']} artifacts")
    return dg.MaterializeResult(
        metadata={"deleted": out["deleted"],
                  **{k: dg.MetadataValue.int(v) for k, v in out["detail"].items()}}
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
    out = ops.system_backup(_admin(context, runner, pipes_subprocess_client), store, runner,
                            _layout, upload=BACKUP_UPLOAD, staging=UPLOAD_STAGING_PATH)
    context.log.info(f"system backup -> {out['key']}")
    return dg.MaterializeResult(metadata={"key": out["key"] or ""})


# --- DBMS metadata export (#14): agentless users/roles/privileges/aliases ---------
@dg.asset(group_name="neo4j_backup")
def metadata_export(
    context: dg.AssetExecutionContext,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
) -> dg.MaterializeResult:
    """Capture the DBMS metadata layer as replayable Cypher (pure Bolt, no runner) and
    store it under the reserved `_dbms/` prefix. Restore is `metadata_restore`."""
    out = ops.export_metadata(neo4j, store, _layout)
    context.log.info(f"metadata export -> {out['key']} ({out['bytes']} bytes)")
    return dg.MaterializeResult(
        metadata={"key": out["key"], "bytes": dg.MetadataValue.int(out["bytes"])})


class MetadataRestoreConfig(dg.Config):
    key: str | None = None  # default: the latest _dbms/ artifact


@dg.op
def metadata_restore_op(
    context: dg.OpExecutionContext,
    config: MetadataRestoreConfig,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
):
    try:
        result = ops.restore_metadata(neo4j, store, _layout, config.key)
    except ops.OpError as e:
        raise dg.Failure(str(e))
    context.log.info(f"replayed {result['applied']} statements from {result['key']}; "
                     f"skipped {len(result['skipped'])}")
    return result


@dg.job
def metadata_restore():
    metadata_restore_op()


# --- Restore (pure Cypher) — alias-swap (default) or by-name (#48) ----------------
class RestoreConfig(dg.Config):
    group_id: str
    restore_until: str | None = None  # ISO-8601; needs a differential chain
    replace: bool = False  # by-name mode only: DROP+recreate a target that already exists


@dg.op
def restore_group_op(
    context: dg.OpExecutionContext,
    config: RestoreConfig,
    neo4j: Neo4jResource,
    store: ObjectStoreResource,
):
    group = load_policy(POLICY_PATH).group(config.group_id)
    try:
        out = ops.restore_group(
            neo4j, store, group, _layout, restore_until=config.restore_until,
            replace=config.replace, cypher_version=SEED_CYPHER_VERSION, log=context.log.info,
        )
    except ops.OpError as e:
        raise dg.Failure(str(e))
    return out["members"]


@dg.job
def restore_group():
    restore_group_op()


# --- Sensor + schedules ----------------------------------------------------------
@dg.sensor(job=backup_job, minimum_interval_seconds=300)
def reconcile_registry(context: dg.SensorEvaluationContext):
    # Force a fresh read — the sensor gates whether a new database gets a partition, so its
    # freshness sets churn latency (#43).
    policy = load_policy(POLICY_PATH, force=True)
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
                # Only request partitions that currently exist — a policy read that's ahead of
                # (or behind) the reconcile sensor must not request a non-existent partition (#43).
                existing = set(context.instance.get_dynamic_partitions(targets.name))
                return [
                    dg.RunRequest(
                        partition_key=f"{g.id}/{a}",
                        tags={"backup_kind": _lane},
                        run_config=dg.RunConfig(ops={"backup": BackupConfig(kind=_kind)}),
                    )
                    for g in pol.groups_for_tier(_tier)
                    for a in g.names
                    if f"{g.id}/{a}" in existing
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
            cloud=os.environ.get("CLOUD") or None,  # aws (default) | azure ; gcp later (#52)
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
