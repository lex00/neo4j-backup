"""A three-server cluster (#234): backup from the secondaries, restore with a TOPOLOGY, and an
alias swap that every member sees. Uses docker/compose.cluster.yaml as cluster `c3`.

1. A database `cl-orders-1` with TOPOLOGY 1 PRIMARY 2 SECONDARIES behind alias `cl-orders`,
   some data, and a group on cluster `c3` whose backup_source is the three backup ports.
2. `neo4j-backup backup` takes a full (neo4j-admin picks a secondary from the list).
3. `neo4j-backup restore --confirm` seeds a new database with the group's topology; every server
   reports it online, and every server's SHOW ALIASES points `cl-orders` at it.

Prereqs: `just fresh`, then
`docker compose --env-file .env -f docker/compose.yaml -f docker/compose.cluster.yaml
 --profile cluster up -d --wait core1 core2 core3`.

    orchestrator/.venv/bin/python orchestrator/smoke_cluster.py
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import yaml
from neo4j import GraphDatabase

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
AUTH = ("neo4j", "devpassword")
ROUTED = "neo4j://localhost:7691"
SERVERS = ["bolt://localhost:7691", "bolt://localhost:7692", "bolt://localhost:7693"]


def q(uri, db, cypher, **kw):
    with GraphDatabase.driver(uri, auth=AUTH) as d, d.session(database=db) as s:
        return [r.data() for r in s.run(cypher, **kw)]


def policy() -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    raw["clusters"] = {"c3": {"bolt_uri": ROUTED,
                              "backup_source": ["core1:6362", "core2:6362", "core3:6362"],
                              "topology": {"primaries": 1, "secondaries": 2}}}
    raw["db_groups"].append({"id": "clgrp", "tier": "gold", "cluster": "c3",
                             "aliases": ["cl-orders"]})
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


def cli(pol, *args):
    env = {**os.environ, "NEO4J_BOLT_URI": "neo4j://localhost:7687", "NEO4J_PASSWORD": AUTH[1],
           "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
           "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
           "BACKUP_BUCKET": "neo4j-backups", "SCRATCH_PATH": "/scratch",
           "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "BACKUP_UPLOAD": "admin",
           "NEO4J_BACKUP_POLICY": pol}
    p = subprocess.run([os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"),
                        "--json", *args], cwd=REPO, env=env, capture_output=True, text=True)
    assert p.returncode == 0, f"[{' '.join(args)}]\n{p.stdout}\n{p.stderr[-2000:]}"
    print(f"== {' '.join(args)}: ok ==")
    return json.loads(p.stdout)["result"]


def wait_online(db, servers=3, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        rows = q(ROUTED, "system", f"SHOW DATABASE `{db}` YIELD serverID, currentStatus "
                                   f"RETURN serverID, currentStatus")
        if len(rows) == servers and all(r["currentStatus"] == "online" for r in rows):
            return rows
        time.sleep(2)
    raise AssertionError(f"{db} not online on {servers} servers: {rows}")


def main() -> None:
    os.chdir(REPO)
    servers = q(ROUTED, "system", "SHOW SERVERS YIELD name, state, health RETURN *")
    assert len(servers) == 3 and all(s["health"] == "Available" for s in servers), servers
    print(f"== cluster: {len(servers)} servers available ==")
    q(ROUTED, "system", "DROP ALIAS `cl-orders` IF EXISTS FOR DATABASE")
    for r in q(ROUTED, "system", "SHOW DATABASES YIELD name WHERE name STARTS WITH 'cl-orders' "
                                 "RETURN DISTINCT name"):
        q(ROUTED, "system", f"DROP DATABASE `{r['name']}` IF EXISTS WAIT")
    q(ROUTED, "system", "CREATE DATABASE `cl-orders-1` TOPOLOGY 1 PRIMARY 2 SECONDARIES WAIT")
    wait_online("cl-orders-1")
    q(ROUTED, "system", "CREATE ALIAS `cl-orders` FOR DATABASE `cl-orders-1`")
    q(ROUTED, "cl-orders", "UNWIND range(1, 5) AS i CREATE (:Cl {i: i})")
    print("== cl-orders-1: 1 primary, 2 secondaries, 5 nodes ==")

    pol = policy()
    try:
        b = cli(pol, "backup", "clgrp", "--kind", "FULL")["backups"][0]
        assert b["taken"] == "full", b
        print(f"== backup from the cluster's backup ports: {b['artifact']} ==")
        r = cli(pol, "restore", "clgrp", "--confirm", "--no-baseline")
        new = r["members"][0]["newdb"]
        rows = wait_online(new)
        roles = q(ROUTED, "system", f"SHOW DATABASE `{new}` YIELD role RETURN role")
        assert sorted(x["role"] for x in roles) == ["primary", "secondary", "secondary"], roles
        print(f"== restored {new}: online on {len(rows)} servers, 1 primary + 2 secondaries ==")
        for uri in SERVERS:
            t = q(uri, "system", "SHOW ALIASES FOR DATABASE YIELD name, database "
                                 "WHERE name = 'cl-orders' RETURN database")
            assert t and t[0]["database"] == new, (uri, t)
        assert q(ROUTED, "cl-orders", "MATCH (n:Cl) RETURN count(n) AS c")[0]["c"] == 5
        print("== every server's SHOW ALIASES points cl-orders at the new database; 5 nodes ==")
        print("PASS: three-server cluster (backup, restore with topology, alias on every member)")
    finally:
        os.unlink(pol)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
