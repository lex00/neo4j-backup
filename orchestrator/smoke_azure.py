"""Azure backend against Azurite (#52, #79, #80): a real backup artifact through our Azure
store code. Upload, server-side copy to a scratch prefix (waits until Azure reports each copy
finished, #80), stream the copy back to disk (#79), and compare bytes. neo4j-admin can't be
pointed at an emulator endpoint, so the aggregate itself isn't run here; verify's copy step is.

Prereqs: `just fresh && just backup demo`, then
`docker compose --env-file .env -f docker/compose.yaml --profile azure up -d azurite`.

    orchestrator/.venv/bin/python orchestrator/smoke_azure.py
"""

import hashlib
import os
import sys
import tempfile

os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"))

from neo4j_backup_core import paths  # noqa: E402
from neo4j_backup_core.clients import AzureObjectStore, object_store  # noqa: E402

CONTAINER = "neo4j-backups-smoke"


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    s3 = object_store("neo4j-backups", "http://localhost:9000", "us-east-1")
    key = s3.latest_artifact_key(paths.alias_prefix("demo", "acme-orders"))
    assert key, "no demo backup: run `just backup demo` first"
    az = AzureObjectStore(CONTAINER)
    from azure.core.exceptions import ResourceExistsError
    try:
        az._c().create_container()
    except ResourceExistsError:
        pass
    with tempfile.TemporaryDirectory() as d:
        orig = os.path.join(d, "orig.backup")
        s3._download(key, orig)
        prefix = key.rsplit("/", 1)[0] + "/"
        az.upload_file(orig, key)
        print(f"== uploaded {key} ({os.path.getsize(orig)} bytes) to Azurite ==")
        n = az.copy_prefix(prefix, f"_verify/smoke/{prefix}")
        dst = f"_verify/smoke/{key}"
        status = az._c().get_blob_client(dst).get_blob_properties().copy.status
        assert n >= 1 and status in (None, "success"), (n, status)
        print(f"== copy_prefix returned after {n} copies; copy status {status} ==")
        back = os.path.join(d, "back.backup")
        az._download(dst, back)
        assert sha(back) == sha(orig), "the streamed copy differs"
        print("== streamed the copy back to disk: bytes identical ==")
        # #79: a large blob streams without being held in memory
        import tracemalloc
        big = os.path.join(d, "big.bin")
        with open(big, "wb") as f:
            for _ in range(256):
                f.write(os.urandom(1 << 20))
        az.upload_file(big, "_verify/smoke/big.bin")
        tracemalloc.start()
        az._download("_verify/smoke/big.bin", os.path.join(d, "big.back"))
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        assert sha(big) == sha(os.path.join(d, "big.back"))
        assert peak < 64 * (1 << 20), f"download peaked at {peak} bytes for a 256 MiB blob"
        print(f"== 256 MiB blob streamed down; peak Python memory {peak // (1 << 20)} MiB ==")
        az.delete_prefix("_verify/smoke/")
        az.delete_keys([key])
    print("PASS: Azure store on Azurite (upload, copy with wait, streamed download)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
