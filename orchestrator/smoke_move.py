"""Restore into another cluster on the local stack (#115), with a second Neo4j (`neo4j-b`,
compose profile `multi`) as cluster `west`:

1. A group `movegrp` on the default cluster: a database behind alias `mv-a`, a role with a grant
   through the alias, a FULL backup.
2. `restore movegrp --to-cluster west` is refused while the source answers; with
   `--fence-source` it seeds on west, replays the role, copies the grant to the new physical,
   creates the alias there and takes the baseline on west.
3. Backups of the group on the default cluster are now refused (fenced); with the policy's
   `cluster: west` they run against west.
4. Lost cluster: export west's metadata, stop `neo4j-b`, and `restore --from-cluster west
   --to-cluster default` brings every west group back from the export and the bucket.

Prereqs: `just fresh`, `docker compose --env-file .env -f docker/compose.yaml --profile multi
up -d --wait neo4j-b`.   orchestrator/.venv/bin/python orchestrator/smoke_move.py
"""

import json
import os
import subprocess
import sys
import tempfile

import yaml
from neo4j import GraphDatabase

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml"]
EXEC = [*COMPOSE, "exec", "-T", "runner"]
A, B = "neo4j://localhost:7687", "neo4j://localhost:7688"
AUTH = ("neo4j", "devpassword")


def policy(cluster: str) -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    raw["clusters"] = {"west": {"bolt_uri": B, "backup_source": "neo4j-b:6362"}}
    raw["db_groups"].append({"id": "movegrp", "tier": "gold", "aliases": ["mv-a"],
                             **({"cluster": cluster} if cluster != "default" else {})})
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


def cli(pol, *args, ok=True) -> dict:
    env = {**os.environ, "NEO4J_BOLT_URI": A, "NEO4J_PASSWORD": AUTH[1],
           "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
           "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
           "BACKUP_BUCKET": "neo4j-backups", "SCRATCH_PATH": "/scratch",
           "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "BACKUP_UPLOAD": "admin",
           "NEO4J_BACKUP_POLICY": pol, "POLICY_CACHE_TTL": "0"}
    p = subprocess.run([os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"),
                        "--json", *args], cwd=REPO, env=env, capture_output=True, text=True)
    print(f"== {' '.join(args)}: exit {p.returncode} ==")
    out = json.loads(p.stdout)
    if ok:
        assert p.returncode == 0, f"{p.stdout}\n{p.stderr[-1500:]}"
    return out


def q(uri, db, cypher, **kw):
    with GraphDatabase.driver(uri, auth=AUTH) as d, d.session(database=db) as s:
        return [r.data() for r in s.run(cypher, **kw)]


def target(uri, alias):
    rows = q(uri, "system", "SHOW ALIASES FOR DATABASE YIELD name, database WHERE name = $a "
                           "RETURN database", a=alias)
    return rows[0]["database"] if rows else None


def main() -> None:
    os.chdir(REPO)
    on_a, on_b = policy("default"), policy("west")
    try:
        q(A, "system", "CREATE DATABASE `mv-a-20261116t000000` IF NOT EXISTS WAIT")
        q(A, "system", "CREATE ALIAS `mv-a` IF NOT EXISTS FOR DATABASE `mv-a-20261116t000000`")
        q(A, "mv-a", "CREATE (:Moved {n: 1}), (:Moved {n: 2})")
        q(A, "system", "CREATE ROLE mover IF NOT EXISTS")
        q(A, "system", "GRANT MATCH {*} ON GRAPH `mv-a` NODE * TO mover")
        cli(on_a, "backup", "movegrp", "--kind", "FULL")
        print("== 1. movegrp on the default cluster: 2 nodes, role mover, a FULL ==")

        refused = cli(on_a, "restore", "movegrp", "--to-cluster", "west", "--dry-run", ok=False)
        assert refused["ok"] is False and "fence-source" in refused["error"]["msg"], refused
        out = cli(on_a, "restore", "movegrp", "--to-cluster", "west", "--fence-source",
                  "--confirm", "--ticket", "SMOKE-115")["result"]
        new = target(B, "mv-a")
        assert new and q(B, "mv-a", "MATCH (n:Moved) RETURN count(n) AS c")[0]["c"] == 2
        roles = {r["role"] for r in q(B, "system", "SHOW ROLES YIELD role")}
        grants = [r["command"] for r in q(B, "system", "SHOW ROLE mover PRIVILEGES AS COMMANDS")]
        assert "mover" in roles and any(f"`{new}`" in g for g in grants), grants
        assert out["baseline"] and out["baseline"][0]["taken"] == "full", out
        assert "cluster: west" in out["policy"], out["policy"]
        print(f"== 2. moved to west: mv-a -> {new}, role and grant copied, baseline on west ==")

        fenced = cli(on_a, "backup", "movegrp", "--kind", "FULL", ok=False)
        assert fenced["ok"] is False and "cluster: west" in fenced["error"]["msg"], fenced
        cli(on_b, "backup", "movegrp", "--kind", "FULL")
        print("== 3. fenced on default; with cluster: west the backup runs against west ==")

        cli(on_b, "metadata", "export", "--cluster", "west")
        subprocess.run([*COMPOSE, "--profile", "multi", "stop", "neo4j-b"], check=True,
                       capture_output=True)
        q(A, "system", "DROP ALIAS `mv-a` IF EXISTS FOR DATABASE")
        q(A, "system", "DROP ROLE mover IF EXISTS")
        back = cli(on_b, "restore", "--from-cluster", "west", "--to-cluster", "default",
                   "--confirm", "--no-baseline")["result"]
        assert back["source_reachable"] is False and back["metadata"]["from"] != "live", back
        restored = target(A, "mv-a")
        assert restored and q(A, "mv-a", "MATCH (n:Moved) RETURN count(n) AS c")[0]["c"] == 2
        assert "mover" in {r["role"] for r in q(A, "system", "SHOW ROLES YIELD role")}
        print(f"== 4. west lost: movegrp back on default (mv-a -> {restored}) from the export ==")
        print("PASS: restore into another cluster (planned move, fence, lost cluster)")
    finally:
        subprocess.run([*COMPOSE, "--profile", "multi", "start", "neo4j-b"],
                       capture_output=True)
        os.unlink(on_a)
        os.unlink(on_b)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
