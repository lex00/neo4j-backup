"""Restore operations end to end on the local stack (#121, #122, #93, #92, #123, #88), through
the CLI with a temporary policy (demo plus two adopt groups and a naming template):

1. status: every demo member is ok after a backup; --stale exits 0.
2. restore --member acme-graph: only that alias moves; the dry-run warns about alignment.
3. rollback: acme-graph goes back to its previous database.
4. cleanup --dry-run lists only superseded physicals the tool made; --confirm drops them.
5. adopt: an existing database gets an alias (alias-only), and a same-name database is
   migrated (--migrate --replace) behind an alias of its own name.
6. a group with naming.physical_template restores into `prod-<slug>-<ts>`.

Prereqs: `just fresh`.   orchestrator/.venv/bin/python orchestrator/smoke_ops.py
"""

import json
import os
import subprocess
import sys
import tempfile

import yaml
from neo4j import GraphDatabase

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
BOLT, AUTH = "neo4j://localhost:7687", ("neo4j", "devpassword")


def policy() -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    raw["db_groups"] += [
        {"id": "adoptalias", "tier": "gold", "aliases": ["legacy-shop"],
         "overrides": {"legacy-shop": {"adopt_from": "shopdb"}}},
        {"id": "adoptsame", "tier": "gold", "aliases": ["samedb"]},
        {"id": "named", "tier": "gold", "aliases": ["named-a"],
         "naming": {"physical_template": "prod-{slug}-{ts}"}},
    ]
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


POL = policy()
ENV = {**os.environ, "NEO4J_BOLT_URI": BOLT, "NEO4J_PASSWORD": AUTH[1],
       "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
       "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
       "BACKUP_BUCKET": "neo4j-backups", "SCRATCH_PATH": "/scratch",
       "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "BACKUP_UPLOAD": "admin",
       "NEO4J_BACKUP_POLICY": POL}


def cli(*args, ok=True) -> dict:
    p = subprocess.run([os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"),
                        "--json", *args], cwd=REPO, env=ENV, capture_output=True, text=True)
    if ok:
        assert p.returncode == 0, f"[{' '.join(args)}]\n{p.stdout}\n{p.stderr[-1500:]}"
    print(f"== {' '.join(args)}: exit {p.returncode} ==")
    return json.loads(p.stdout)


def sysq(q, **kw):
    with GraphDatabase.driver(BOLT, auth=AUTH) as d, d.session(database="system") as s:
        return [r.data() for r in s.run(q, **kw)]


def target(alias):
    rows = sysq("SHOW ALIASES FOR DATABASE YIELD name, database WHERE name = $a "
                "RETURN database", a=alias)
    return rows[0]["database"] if rows else None


def reset() -> None:
    """Start clean, so the smoke can be run again on the same stack."""
    for alias in ("legacy-shop", "samedb", "named-a"):
        sysq(f"DROP ALIAS `{alias}` IF EXISTS FOR DATABASE")
    for r in sysq("SHOW DATABASES YIELD name WHERE name IN ['shopdb', 'samedb', 'customer-made'] "
                  "OR name STARTS WITH 'samedb-' OR name STARTS WITH 'named-a' "
                  "OR name STARTS WITH 'prod-named-a-' RETURN DISTINCT name"):
        sysq(f"DROP DATABASE `{r['name']}` IF EXISTS WAIT")


def main() -> None:
    reset()
    cli("backup", "demo", "--kind", "FULL")
    st = cli("status", "demo", "--stale")["result"]
    assert {r["state"] for r in st["targets"]} == {"ok"}, st
    print("== 1. status: every demo member ok ==")

    before = {a: target(a) for a in ("acme-orders", "acme-graph", "acme-audit")}
    plan = cli("restore", "demo", "--member", "acme-graph", "--dry-run")["result"]["plan"]
    assert "no longer be aligned" in plan["warning"], plan
    cli("restore", "demo", "--member", "acme-graph", "--confirm", "--no-baseline")
    after = {a: target(a) for a in before}
    assert after["acme-graph"] != before["acme-graph"]
    assert after["acme-orders"] == before["acme-orders"] and \
        after["acme-audit"] == before["acme-audit"]
    print(f"== 2. restore --member: only acme-graph moved ({after['acme-graph']}) ==")

    cli("rollback", "demo", "--member", "acme-graph", "--confirm")
    assert target("acme-graph") == before["acme-graph"]
    print(f"== 3. rollback: acme-graph back on {before['acme-graph']} ==")

    sysq("CREATE DATABASE `customer-made` IF NOT EXISTS WAIT")
    plan = cli("cleanup", "demo", "--dry-run")["result"]["plan"]
    drops = plan["drops"]
    graph = next(m for m in plan["members"] if m["alias"] == "acme-graph")
    assert graph["kept"] == [after["acme-graph"]], plan  # --keep 1: the rollback target stays
    assert "customer-made" not in drops, plan
    assert not set(drops) & set(before.values()), plan  # never a current target
    cli("cleanup", "demo", "--confirm")
    names = {r["name"] for r in sysq("SHOW DATABASES YIELD name")}
    assert not set(drops) & names and "customer-made" in names
    print(f"== 4. cleanup dropped {drops}; kept {graph['kept']} for rollback; "
          f"customer-made kept ==")

    sysq("CREATE DATABASE shopdb IF NOT EXISTS WAIT")
    plan = cli("adopt", "adoptalias", "--dry-run")["result"]["plan"]
    assert plan["members"][0]["action"] == "create", plan
    cli("adopt", "adoptalias", "--confirm")
    assert target("legacy-shop") == "shopdb"
    print("== 5a. adopt: legacy-shop -> shopdb (alias only) ==")

    sysq("CREATE DATABASE samedb IF NOT EXISTS WAIT")
    with GraphDatabase.driver(BOLT, auth=AUTH) as d, d.session(database="samedb") as s:
        s.run("CREATE (:Same {n: 1}), (:Same {n: 2})").consume()
    refused = cli("adopt", "adoptsame", "--confirm", ok=False)
    assert refused["ok"] is False, refused
    out = cli("adopt", "adoptsame", "--migrate", "--replace", "--confirm")["result"]
    new = out["members"][0]["newdb"]
    assert target("samedb") == new
    with GraphDatabase.driver(BOLT, auth=AUTH) as d, d.session(database="samedb") as s:
        assert s.run("MATCH (n:Same) RETURN count(n) AS c").single()["c"] == 2
    print(f"== 5b. adopt --migrate: samedb is now an alias for {new}, data kept ==")

    sysq("CREATE DATABASE `named-a-seed` IF NOT EXISTS WAIT")
    sysq("CREATE ALIAS `named-a` IF NOT EXISTS FOR DATABASE `named-a-seed`")
    cli("backup", "named", "--kind", "FULL")
    r = cli("restore", "named", "--confirm", "--no-baseline")["result"]
    assert r["members"][0]["newdb"].startswith("prod-named-a-"), r
    assert target("named-a") == r["members"][0]["newdb"]
    print(f"== 6. naming template: named-a -> {r['members'][0]['newdb']} ==")
    print("PASS: restore --member, rollback, cleanup, adopt, status, naming template")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        os.unlink(POL)
