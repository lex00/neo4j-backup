"""End-to-end smoke of bulk import (#16) — the real off-cluster pattern against the local stack.

A throwaway "loader" container stands in for ephemeral hardware: `neo4j-backup import` loads a tiny
CSV into the loader's **default database before it ever starts** (the reliable adoption path — see
below), the loader starts and serves the data, it's online-backed-up to a native `.backup`, and the
main cluster **seeds a copy from that artifact**. The node count survives the whole trip.

Why a fresh loader and the default db: on current Neo4j, `neo4j-admin database import full` into an
*already-registered* database (CREATE-then-import, or a running DBMS) **quarantines** the store
(store-ID mismatch). Importing into the default db of a never-started instance is the path that
adopts cleanly. IMPORT.md documents this; this smoke proves it.

Prereqs: the stack up (`just fresh`), Docker, and `NEO4J_VERSION` in the environment (`.env`).

    just import-smoke
"""

import json
import os
import subprocess
import sys
import tempfile
import time

from neo4j_backup_core.clients import Neo4jClient, object_store

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup")
LOADER = "neo4j-import-loader"
PW = "loaderpass123"
PREFIX = "_import/smoke/"
COPY = "importsmokeseed"
CSV = ":ID,name\n1,alice\n2,bob\n"  # 2 nodes


# run inside the loader as the neo4j user (neo4j-admin refuses to touch neo4j-owned files as root)
EXEC = ["docker", "exec", "-u", "neo4j", LOADER]


def dex(*argv, **kw):
    return subprocess.run([*EXEC, *argv], check=True, **kw)


def main() -> None:
    version = os.environ["NEO4J_VERSION"]
    pw = os.environ.get("NEO4J_PASSWORD", "devpassword")
    neo = Neo4jClient(os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"), "neo4j", pw)
    store = object_store(os.environ.get("BACKUP_BUCKET", "neo4j-backups"),
                         "http://localhost:9000", os.environ.get("AWS_REGION", "us-east-1"))
    neo.drop_database(COPY)
    store.delete_prefix(PREFIX)
    subprocess.run(["docker", "rm", "-f", LOADER], capture_output=True)

    try:
        # a fresh loader with the server NOT started (sleep entrypoint) and the backup port open
        subprocess.run([
            "docker", "run", "-d", "--name", LOADER,
            "-e", "NEO4J_ACCEPT_LICENSE_AGREEMENT=eval",
            "-e", "NEO4J_server_backup_enabled=true",
            "-e", "NEO4J_server_backup_listen__address=0.0.0.0:6362",
            "--entrypoint", "sleep", f"neo4j:{version}", "infinity",
        ], check=True, capture_output=True)
        dex("sh", "-c", f"printf '{CSV}' > /tmp/nodes.csv")

        # 1) import via the CLI (RUNNER_EXEC_PREFIX -> the loader) into the default db, pre-start
        env = {**os.environ, "RUNNER_EXEC_PREFIX": json.dumps(EXEC)}
        r = subprocess.run([CLI, "--json", "import", "neo4j", "--nodes=/tmp/nodes.csv"],
                           env=env, cwd=REPO, capture_output=True, text=True)
        assert r.returncode == 0, f"import failed:\n{r.stderr}"
        print(f"== import ok: {json.loads(r.stdout)['result']['database']} ==")

        # 2) start the loader; the default db adopts the imported store.
        # (--entrypoint sleep skipped the image's NEO4J_* env -> conf translation, so enable backup
        # in neo4j.conf directly.)
        dex("sh", "-c", "printf 'server.backup.enabled=true\\nserver.backup.listen_address=0.0.0.0:6362\\n'"
                        " >> /var/lib/neo4j/conf/neo4j.conf")
        dex("neo4j-admin", "dbms", "set-initial-password", PW, capture_output=True)
        subprocess.run(["docker", "exec", "-d", "-u", "neo4j", LOADER, "neo4j", "start"], check=True)
        for _ in range(40):
            if subprocess.run(["docker", "exec", LOADER, "cypher-shell", "-u", "neo4j", "-p", PW,
                               "RETURN 1"], capture_output=True).returncode == 0:
                break
            time.sleep(2)
        got = dex("cypher-shell", "-u", "neo4j", "-p", PW, "-d", "neo4j",
                  "MATCH (n) RETURN count(n) AS n", capture_output=True, text=True).stdout
        assert "2" in got, f"imported db not online with 2 nodes: {got}"
        print("== loader started: imported store adopted, 2 nodes online ==")

        # 3) online-backup the loader to a native .backup, then upload it to the object store
        dex("mkdir", "-p", "/tmp/bk")
        bk = subprocess.run([*EXEC, "neo4j-admin", "database", "backup", "--from", "localhost:6362",
                             "--to-path", "/tmp/bk", "--type", "FULL", "neo4j"],
                            capture_output=True, text=True)
        assert bk.returncode == 0, f"loader backup failed:\n{bk.stderr}"
        name = dex("sh", "-c", "ls /tmp/bk/*.backup | head -1 | xargs -n1 basename",
                   capture_output=True, text=True).stdout.strip()
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, name)
            subprocess.run(["docker", "cp", f"{LOADER}:/tmp/bk/{name}", local], check=True,
                           capture_output=True)
            key = f"{PREFIX}{name}"
            store.upload_file(local, key)
        print(f"== backed up loader -> {key} ==")

        # 4) the main cluster seeds a copy from that artifact — the count must survive
        neo.seed_database(COPY, store.uri(key))
        for _ in range(30):
            try:
                if neo.count_nodes(COPY) == 2:
                    break
            except Exception:
                pass
            time.sleep(1)
        assert neo.count_nodes(COPY) == 2, "seeded copy did not have 2 nodes"
        print(f"== main cluster seeded {COPY} from the imported artifact: 2 nodes ==")
        print("PASS: off-cluster import -> adopt -> backup -> seed (node count preserved)")
    finally:
        subprocess.run(["docker", "rm", "-f", LOADER], capture_output=True)
        neo.drop_database(COPY)
        store.delete_prefix(PREFIX)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
