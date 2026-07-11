"""Validate restore **cloud-agnostically** via `file://` seed (#52 phase 4).

A `.backup` is byte-identical regardless of which cloud stored it, and the cloud-specific part
of restore (the Neo4j server fetching `s3://`/`gs://`/`azb://`) is Neo4j's code — not ours, and
not emulatable for GCS. So we validate *our* restore drive from a local file: download the
artifact from whatever backend is configured (S3/MinIO, Azure/Azurite, GCS/fake-gcs) onto the
neo4j container's `/seed` mount, then `CREATE DATABASE … seedURI: 'file:/seed/…'`.

Prereqs: the stack up with an artifact (`just fresh && just backup demo`), the neo4j service
with `FileSeedProvider` enabled + `../.seed:/seed` mounted (see docker/compose.yaml).

    orchestrator/.venv/bin/python orchestrator/smoke_file_restore.py

Set CLOUD (+ that backend's env) to exercise a non-S3 backend; the file:// restore is identical.
"""

import os

from neo4j_backup_core import paths
from neo4j_backup_core.clients import Neo4jClient, object_store

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_DIR = os.path.join(REPO, ".seed")  # host side of the neo4j container's /seed mount
ALIAS = "acme-orders"


def main() -> None:
    store = object_store(
        os.environ.get("BACKUP_BUCKET", "neo4j-backups"),
        os.environ.get("AWS_ENDPOINT_URL_S3", "http://localhost:9000") or None,
        os.environ.get("AWS_REGION", "us-east-1"),
        cloud=os.environ.get("CLOUD") or None,
    )
    neo = Neo4jClient(
        os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"), "neo4j",
        os.environ.get("NEO4J_PASSWORD", "devpassword"),
    )

    src = store.latest_artifact_key(paths.alias_prefix("demo", ALIAS))
    assert src, f"no artifact for demo/{ALIAS} — run `just backup demo` first"
    name = os.path.basename(src)

    # download from the configured cloud backend onto the container's /seed mount
    os.makedirs(SEED_DIR, exist_ok=True)
    local = os.path.join(SEED_DIR, name)
    store._download(src, local)
    os.chmod(local, 0o644)  # readable by the neo4j process in the container
    print(f"== staged {name} from {type(store).__name__} -> /seed ==")

    db = "fileseedtest"
    neo.drop_database(db)
    try:
        neo.seed_database(db, f"file:/seed/{name}")  # FileSeedProvider — no cloud fetch
        n = neo.count_nodes(db)
        assert n > 0, f"expected restored data, got {n}"
        print(f"== file:// restore OK: {db} has {n} nodes (restore drive validated, any cloud) ==")
    finally:
        neo.drop_database(db)
        os.remove(local)
    print("PASS: cloud-agnostic restore via file:// seed")


if __name__ == "__main__":
    main()
