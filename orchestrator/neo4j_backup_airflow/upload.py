"""BACKUP_UPLOAD=pipeline helpers for the Airflow DAGs — thin wrappers over
`neo4j_backup_core.ops`, binding Airflow's subprocess/KPO `run_admin` and reading the
`BACKUP_UPLOAD`/`UPLOAD_STAGING_PATH` env so the DAGs keep their existing call sites.

The admin-vs-pipeline routing (neo4j-admin has no S3 SSE setting, so for a bucket that denies
header-less PutObject the pipeline stages locally and lets boto3 do the SSE-KMS writes) now lives
in core, shared with the Dagster adapter and the CLI. Reads stay direct (#39).
"""

import os

from neo4j_backup_core import ops
from neo4j_backup_airflow.execution import run_admin

BACKUP_UPLOAD = os.environ.get("BACKUP_UPLOAD", "admin")
STAGING = os.environ.get("UPLOAD_STAGING_PATH") or None


def run_backup(runner, store, database, prefix, kind):
    return ops.run_backup(run_admin, runner, store, database, prefix, kind,
                          upload=BACKUP_UPLOAD, staging=STAGING)


def run_aggregate(runner, store, physical, prefix):
    return ops.run_aggregate(run_admin, runner, store, physical, prefix,
                             upload=BACKUP_UPLOAD, staging=STAGING)


def run_verify(runner, store, physical, src, scratch):
    ops.run_verify(run_admin, runner, store, physical, src, scratch,
                   upload=BACKUP_UPLOAD, staging=STAGING)
