"""`load` end to end on the local stack (#107): CSV and Parquet builds, read-only and read-write
groups.

For each case it writes the source files to the group's `_source/` folder in the bucket, runs
`neo4j-backup load <group> --dry-run` then `--confirm`, and checks: the alias serves the loaded
counts, the database's access matches `writes`, the dump sits in the member's `_seed/`, and a
second load swaps to another new database. Groups come from a temporary policy (demo + 4).

Prereqs: `just fresh`; the orchestrator venv with the dev extra (pyarrow, for Parquet).

    just load-smoke   # or: orchestrator/.venv/bin/python orchestrator/smoke_load.py
"""

import io
import json
import os
import subprocess
import sys
import tempfile

import yaml
from neo4j import GraphDatabase

from neo4j_backup_core.clients import object_store

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup")
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
BOLT = os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687")
AUTH = ("neo4j", os.environ.get("NEO4J_PASSWORD", "devpassword"))
BUCKET = os.environ.get("BACKUP_BUCKET", "neo4j-backups")
NODES_CSV = "id:ID,name,:LABEL\n1,a,Item\n2,b,Item\n3,c,Item\n"
RELS_CSV = ":START_ID,:END_ID,:TYPE\n1,2,LINKS\n2,3,LINKS\n"

CASES = [  # group, alias, input type, writes
    ("ldcsvro", "ld-csv-ro", "csv", "read-only"),
    ("ldcsvrw", "ld-csv-rw", "csv", "read-write"),
    ("ldpqro", "ld-pq-ro", "parquet", "read-only"),
    ("ldpqrw", "ld-pq-rw", "parquet", "read-write"),
]


CMD = ("if [ \"$FAIL\" = 1 ]; then echo 'build failed on purpose' >&2; exit 3; fi; "
       "printf 'id:ID,name,:LABEL\\n1,a,Item\\n2,b,Item\\n3,c,Item\\n' > nodes.csv && "
       "printf ':START_ID,:END_ID,:TYPE\\n1,2,LINKS\\n2,3,LINKS\\n' > rels.csv && "
       "echo SMOKE109-converted")


def policy_file(fail: bool = False) -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    for gid, alias, kind, writes in CASES:
        ext = "csv" if kind == "csv" else "parquet"
        load = {"mode": "rebuild", "writes": writes,
                "build": {"kind": "import", "input_type": kind,
                          "args": [f"--nodes=nodes.{ext}", f"--relationships=rels.{ext}"]},
                "checks": [{"query": "MATCH (i:Item) RETURN count(i)", "expect": "== 3"}]}
        if writes == "read-write":
            load["protect_writes"] = True
        raw["db_groups"].append({"id": gid, "aliases": [alias], "tier": "both", "load": load})
    # #109: a command build (the RDF-style "convert, then import" shape) in the runner
    raw["db_groups"].append({"id": "ldcmd", "aliases": ["ld-cmd"], "tier": "both", "load": {
        "mode": "rebuild", "writes": "read-only",
        "build": {"kind": "command", "command": ["sh", "-c", CMD], "output": "import",
                  "import_args": ["--nodes=nodes.csv", "--relationships=rels.csv"],
                  "env": {"FAIL": "1" if fail else "0"}, "timeout": "5m"},
        "checks": [{"query": "MATCH (i:Item) RETURN count(i)", "expect": "== 3"}]}})
    raw["tiers"]["both"] = {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *",
                            "rebuild_cron": "0 1 * * *"}
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


def parquet(rows: list[dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


def put_sources(store, gid: str, kind: str) -> None:
    if kind == "csv":
        store.put_text(f"{gid}/_source/nodes.csv", NODES_CSV)
        store.put_text(f"{gid}/_source/rels.csv", RELS_CSV)
        return
    with tempfile.TemporaryDirectory() as d:
        for name, rows in (("nodes", [{"id:ID": str(i), "name": n, ":LABEL": "Item"}
                                      for i, n in ((1, "a"), (2, "b"), (3, "c"))]),
                           ("rels", [{":START_ID": "1", ":END_ID": "2", ":TYPE": "LINKS"},
                                     {":START_ID": "2", ":END_ID": "3", ":TYPE": "LINKS"}])):
            path = os.path.join(d, f"{name}.parquet")
            open(path, "wb").write(parquet(rows))
            store.upload_file(path, f"{gid}/_source/{name}.parquet")


def cli(pol: str, *args) -> dict:
    env = {**os.environ, "NEO4J_BOLT_URI": BOLT, "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
           "SCRATCH_PATH": "/scratch", "RUNNER_EXEC_PREFIX": json.dumps(EXEC),
           "BACKUP_UPLOAD": "admin", "NEO4J_BACKUP_POLICY": pol}
    p = subprocess.run([CLI, "--json", *args], cwd=REPO, env=env, capture_output=True, text=True)
    assert p.returncode == 0, f"[{' '.join(args)}] exit {p.returncode}\n{p.stdout}\n{p.stderr[-2000:]}"
    print(f"== {' '.join(args)}: ok ==")
    return json.loads(p.stdout)["result"]


def cli_raw(pol: str, *args):
    env = {**os.environ, "NEO4J_BOLT_URI": BOLT, "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
           "SCRATCH_PATH": "/scratch", "RUNNER_EXEC_PREFIX": json.dumps(EXEC),
           "BACKUP_UPLOAD": "admin", "NEO4J_BACKUP_POLICY": pol}
    return subprocess.run([CLI, "--json", *args], cwd=REPO, env=env, capture_output=True,
                          text=True)


def alias_db(alias: str) -> str:
    return q("system", f"SHOW ALIASES FOR DATABASE YIELD name, database WHERE name = '{alias}' "
                       f"RETURN database")[0]["database"]


def command_build(pol: str) -> None:
    """#109 on the stack: the command's output is imported; its logs reach the CLI's stderr;
    a failing command stops the load before any seed and leaves the alias where it was (#107)."""
    p = cli_raw(pol, "load", "ldcmd", "--confirm", "--ticket", "SMOKE-109")
    assert p.returncode == 0, f"command build failed\n{p.stdout}\n{p.stderr[-3000:]}"
    m = json.loads(p.stdout)["result"]["members"][0]
    assert m["import_counts"] == {"nodes": 3, "relationships": 2}, m
    assert q("ld-cmd", "MATCH (i:Item) RETURN count(i) AS n")[0]["n"] == 3
    assert "SMOKE109-converted" in p.stderr, p.stderr[-2000:]
    print("== command build: converted, imported, 3 nodes via ld-cmd; its log is in stderr ==")
    bad = policy_file(fail=True)
    try:
        before = alias_db("ld-cmd")
        dbs = {r["name"] for r in q("system", "SHOW DATABASES YIELD name")}
        f = cli_raw(bad, "load", "ldcmd", "--confirm")
        assert f.returncode != 0, f.stdout
        env = json.loads(f.stdout)
        assert env["ok"] is False and "no alias was moved" in env["error"]["msg"], env
        assert alias_db("ld-cmd") == before
        assert {r["name"] for r in q("system", "SHOW DATABASES YIELD name")} == dbs
        assert "build failed on purpose" in f.stderr, f.stderr[-2000:]
        print(f"== failing build: {env['error']['msg'][:100]}; alias still on {before} ==")
    finally:
        os.unlink(bad)


def q(db: str, cypher: str):
    with GraphDatabase.driver(BOLT, auth=AUTH) as d, d.session(database=db) as s:
        return [r.data() for r in s.run(cypher)]


def main() -> None:
    pol = policy_file()
    store = object_store(BUCKET, "http://localhost:9000")
    try:
        for gid, alias, kind, writes in CASES:
            put_sources(store, gid, kind)
            plan = cli(pol, "load", gid, "--dry-run")["plan"]["members"][0]
            assert plan["objects"], plan
            out = cli(pol, "load", gid, "--confirm", "--ticket", "SMOKE-LOAD")
            m = out["members"][0]
            assert m["import_counts"] == {"nodes": 3, "relationships": 2}, m
            assert q(alias, "MATCH (i:Item) RETURN count(i) AS n")[0]["n"] == 3
            access = q("system", f"SHOW DATABASE `{m['newdb']}` YIELD access RETURN access")
            assert access[0]["access"] == writes, (access, writes)
            assert m["dump"].startswith(f"{gid}/") and "/_seed/" in m["dump"]
            again = cli(pol, "load", gid, "--confirm")["members"][0]
            assert again["newdb"] != m["newdb"] and again["old"] == m["newdb"]
            base = out.get("baseline") or []
            if writes == "read-write":
                assert base and base[0]["taken"] == "full", base
            print(f"== {kind} {writes}: 3 nodes, 2 relationships via {alias}, access {writes} ==")
        command_build(pol)
        print("PASS: load (CSV and Parquet, read-only and read-write; a command build)")
    finally:
        os.unlink(pol)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
