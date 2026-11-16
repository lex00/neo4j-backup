"""The dump path end to end (#161, epic #98) against the local stack:

import -> dump -> seed -> baseline FULL -> write -> DIFF -> point-in-time restore -> export.

1. `import --to-dump demo/acme-orders` on the runner (a never-started store, 3 nodes).
2. `restore demo --from-dump acme-orders=<key> --confirm`: acme-orders has 3 nodes; the baseline
   full of each restored database is taken.
3. Write node 4, note T, write node 5, `backup --kind DIFF`: the diff lands on the baseline.
4. `restore demo --until T --confirm`: acme-orders has 4 nodes (node 5 is after T).
5. `export demo --until T --confirm`: the acme-orders dump seeds a copy with 4 nodes.

Prereqs: `just fresh` (it takes a full backup of the group first).

    just dump-smoke   # or: orchestrator/.venv/bin/python orchestrator/smoke_dump.py
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

from neo4j import GraphDatabase

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup")
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
BOLT = os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687")
AUTH = ("neo4j", os.environ.get("NEO4J_PASSWORD", "devpassword"))
ENV = {**os.environ, "NEO4J_BOLT_URI": BOLT, "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
       "SCRATCH_PATH": "/scratch", "RUNNER_EXEC_PREFIX": json.dumps(EXEC),
       "BACKUP_UPLOAD": "admin", "NEO4J_BACKUP_POLICY": "policies/demo.yaml"}


def cli(*args) -> dict:
    p = subprocess.run([CLI, "--json", *args], cwd=REPO, env=ENV, capture_output=True, text=True)
    assert p.returncode == 0, f"[{' '.join(args)}] exit {p.returncode}\n{p.stderr[-2000:]}"
    out = json.loads(p.stdout)
    assert out["ok"], out
    print(f"== {' '.join(args)}: ok ==")
    return out["result"]


def cypher(db: str, q: str, **params):
    with GraphDatabase.driver(BOLT, auth=AUTH) as d, d.session(database=db) as s:
        return [r.data() for r in s.run(q, **params)]


def count(db: str) -> int:
    return cypher(db, "MATCH (n:Dump161) RETURN count(n) AS n")[0]["n"]


def main() -> None:
    subprocess.run([*EXEC, "sh", "-c",
                    "printf ':ID,n:int,:LABEL\\n1,1,Dump161\\n2,2,Dump161\\n3,3,Dump161\\n' "
                    "> /tmp/dump161.csv"], cwd=REPO, check=True)
    cli("backup", "demo", "--kind", "FULL")  # the other members restore from their backups
    key = cli("import", "--to-dump", "demo/acme-orders", "dump161",
              "--nodes=/tmp/dump161.csv")["key"]

    out = cli("restore", "demo", "--from-dump", f"acme-orders={key}", "--confirm",
              "--ticket", "SMOKE-161")
    assert count("acme-orders") == 3, count("acme-orders")
    assert [b["taken"] for b in out["baseline"]] == ["full"] * 3, out["baseline"]
    print("== seeded acme-orders from the dump (3 nodes); baseline fulls taken ==")

    cypher("acme-orders", "CREATE (:Dump161 {n: 4})")
    # T from the server clock, then wait until the server is strictly past it (the `system`
    # database refuses a bare RETURN, so ask the default database)
    # --until works to the second, so T is the next whole second after node 4's commit
    now = cypher("neo4j", "RETURN toString(datetime()) AS t")[0]["t"][:19]
    t = (datetime.fromisoformat(now) + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
    while not cypher("neo4j", "RETURN datetime() > datetime($t) AS ok", t=t + "Z")[0]["ok"]:
        time.sleep(0.1)
    cypher("acme-orders", "CREATE (:Dump161 {n: 5})")
    diff = cli("backup", "demo", "--kind", "DIFF")
    taken = {b["alias"]: b["taken"] for b in diff["backups"]}
    assert taken["acme-orders"] == "diff" and diff["needs_full"] == [], diff
    print(f"== T={t}; node 5 after T; DIFF on top of the baseline ==")

    cli("restore", "demo", "--until", t, "--confirm", "--no-baseline")
    assert count("acme-orders") == 4, count("acme-orders")
    print("== point-in-time restore to T: 4 nodes ==")

    exported = cli("export", "demo", "--until", t, "--confirm")
    k = next(e["key"] for e in exported["exports"] if e["member"] == "acme-orders")
    cypher("system", "DROP DATABASE dumpcheck161 IF EXISTS WAIT")
    cypher("system", f"CREATE DATABASE dumpcheck161 OPTIONS {{seedURI: "
                     f"'s3://{os.environ.get('BACKUP_BUCKET', 'neo4j-backups')}/{k}'}} WAIT")
    try:
        assert count("dumpcheck161") == 4, count("dumpcheck161")
    finally:
        cypher("system", "DROP DATABASE dumpcheck161 IF EXISTS WAIT")
    print("== export --until T seeds a copy with 4 nodes ==")
    print("PASS: import -> dump -> seed -> baseline -> diff -> point-in-time restore -> export")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e!r}", file=sys.stderr)
        sys.exit(1)
