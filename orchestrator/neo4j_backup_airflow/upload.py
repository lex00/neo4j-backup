"""BACKUP_UPLOAD=pipeline helpers shared by the Airflow DAGs.

neo4j-admin has no S3 server-side-encryption setting, so for a bucket that denies header-less
PutObject its `--to-path s3://…` write is rejected. `BACKUP_UPLOAD=pipeline` (subprocess mode)
runs neo4j-admin against local paths and lets boto3 do the S3 writes with `write_args` (SSE-KMS).
Mirrors the Dagster `_run_backup` / `_run_aggregate` / verify logic; reads (`--from-path s3://…`)
stay direct (GET is transparent with kms:Decrypt).
"""

import os
import shutil

from neo4j_backup_airflow.execution import run_admin

BACKUP_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
_STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None


def _stage(runner, tag, name):
    return f"{(_STAGING or runner.scratch_path).rstrip('/')}/{tag}/{name}"


def run_backup(runner, store, database, prefix, kind):
    if BACKUP_UPLOAD == "pipeline":
        stage = _stage(runner, "_stage", database)
        os.makedirs(stage, exist_ok=True)
        run_admin(runner.backup_command(database, stage, kind=kind))
        return store.upload_backups(stage, prefix)
    run_admin(runner.backup_command(database, store.uri(prefix), kind=kind))
    return store.latest_artifact_key(prefix)


def run_aggregate(runner, store, physical, prefix):
    if BACKUP_UPLOAD == "pipeline":
        stage = _stage(runner, "_agg", physical)
        store.download_prefix(prefix, stage)
        run_admin(runner.aggregate_command(physical, stage))
        return store.sync_up(stage, prefix)
    run_admin(runner.aggregate_command(physical, store.uri(prefix)))
    return store.latest_artifact_key(prefix)


def run_verify(runner, store, physical, src, scratch):
    if BACKUP_UPLOAD == "pipeline":
        stage = _stage(runner, "_verify", physical)
        store.download_prefix(src, stage)  # verify on local disk — no S3 scratch writes
        try:
            run_admin(runner.aggregate_command(physical, stage))
            full = next(f for f in sorted(os.listdir(stage)) if f.endswith(".backup"))
            run_admin(runner.check_command(physical, os.path.join(stage, full)))
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        return
    try:
        store.copy_prefix(src, scratch)
        run_admin(runner.aggregate_command(physical, store.uri(scratch)))
        full = store.latest_artifact_key(scratch)
        run_admin(runner.check_command(physical, store.uri(full)))
    finally:
        store.delete_prefix(scratch)
