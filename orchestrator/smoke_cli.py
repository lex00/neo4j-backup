"""End-to-end smoke of the `neo4j-backup` CLI (#58 P1/P2) against the local compose stack.

Drives the installed console script through a full loop — targets, backup, verify, aggregate,
restore, metadata export/restore, system-backup, prune — plus a guard check (restore without
--confirm must refuse). Every command is run with `--json` and asserted against the #60 contract
(`validate_envelope`) with the expected exit code.

Execution model: the CLI runs on the host (Bolt + S3 over the mapped localhost ports), and
neo4j-admin runs inside the `runner` container via `RUNNER_EXEC_PREFIX` — the same
`docker compose exec -T runner` bridge the bootstrap scripts use. `BACKUP_UPLOAD=admin` (neo4j-admin
writes straight to s3://) because pipeline staging would land in the container, unreadable from the
host; pipeline mode is for a CLI that runs *on* the runner with a local neo4j-admin.

Prereqs: `just fresh && just backup demo` (a stack with the demo group bootstrapped).

    just cli-smoke        # or: orchestrator/.venv/bin/python orchestrator/smoke_cli.py
"""

import json
import os
import subprocess
import sys
import tempfile

from neo4j_backup_core.cli_contract import Exit, validate_envelope

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup")

# The CLI runs on the host; neo4j-admin execs inside the runner container.
EXEC_PREFIX = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml",
               "exec", "-T", "runner"]

ENV = {
    **os.environ,
    "NEO4J_BOLT_URI": os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"),
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000",   # host-side MinIO
    "SCRATCH_PATH": "/scratch",                        # container path (neo4j-admin --temp-path)
    "RUNNER_EXEC_PREFIX": json.dumps(EXEC_PREFIX),
    "BACKUP_UPLOAD": "admin",
    "NEO4J_BACKUP_POLICY": "policies/demo.yaml",
}


def run_cli(*args, expect_ok=True, expect_exit=Exit.OK):
    proc = subprocess.run([CLI, "--json", *args], cwd=REPO, env=ENV,
                          capture_output=True, text=True)
    label = " ".join(args)
    assert proc.returncode == int(expect_exit), \
        f"[{label}] exit {proc.returncode} != {int(expect_exit)}\nstderr: {proc.stderr}"
    obj = json.loads(proc.stdout)
    problems = validate_envelope(obj)
    assert not problems, f"[{label}] envelope violates the contract: {problems}\n{obj}"
    assert obj["ok"] is expect_ok, f"[{label}] ok={obj['ok']} != {expect_ok}\n{obj}"
    print(f"== {label}: ok (exit {proc.returncode}) ==")
    return obj


# --- #117: a restricted customer user keeps access across a restore ---------------------------

BOLT = ENV["NEO4J_BOLT_URI"]
ADMIN = ("neo4j", os.environ.get("NEO4J_PASSWORD", "devpassword"))
CUSTOMER = ("smoke117", "smoke117-pass")


def _system(cypher: str) -> None:
    from neo4j import GraphDatabase

    with GraphDatabase.driver(BOLT, auth=ADMIN) as d, d.session(database="system") as s:
        s.run(cypher).consume()


def _write(database: str, cypher: str) -> None:
    from neo4j import GraphDatabase

    with GraphDatabase.driver(BOLT, auth=ADMIN) as d, d.session(database=database) as s:
        s.run(cypher).consume()


def _customer_reads(database: str) -> int:
    """Count nodes in `database` as the restricted user; raises on access denied."""
    from neo4j import GraphDatabase

    with GraphDatabase.driver(BOLT, auth=CUSTOMER) as d, d.session(database=database) as s:
        return s.run("MATCH (n) RETURN count(n) AS n").single()["n"]


def setup_customer(database: str) -> None:
    """A role with ACCESS + MATCH on one database (or alias), and a user holding only that role."""
    for stmt in (
        "CREATE ROLE smoke117 IF NOT EXISTS",
        f"CREATE USER {CUSTOMER[0]} IF NOT EXISTS SET PASSWORD '{CUSTOMER[1]}' CHANGE NOT REQUIRED",
        "GRANT ROLE smoke117 TO smoke117",
        f"GRANT ACCESS ON DATABASE `{database}` TO smoke117",
        f"GRANT MATCH {{*}} ON GRAPH `{database}` TO smoke117",
    ):
        _system(stmt)


def by_name_privileges() -> None:
    """by-name restore drops and recreates the database; its privileges must come back (#117)."""
    pol = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    pol.write("db_groups:\n  - id: smoke117\n    restore_mode: by-name\n"
              "    databases: [smoke117db]\n    tier: gold\n"
              "    s3_prefix: s3://neo4j-backups/smoke117/\n    retention_days: 7\n"
              "tiers:\n  gold: { full_cron: '0 2 * * *', diff_cron: '0 * * * *' }\n")
    pol.close()
    old = ENV["NEO4J_BACKUP_POLICY"]
    ENV["NEO4J_BACKUP_POLICY"] = pol.name
    try:
        _system("CREATE DATABASE smoke117db IF NOT EXISTS WAIT")
        setup_customer("smoke117db")
        _customer_reads("smoke117db")
        run_cli("backup", "smoke117")
        out = run_cli("restore", "smoke117", "--replace", "--confirm")
        assert out["result"]["members"][0]["privileges"], out
        _customer_reads("smoke117db")
        print("== by-name restore kept the customer's privileges ==")
    finally:
        ENV["NEO4J_BACKUP_POLICY"] = old
        _system("DROP DATABASE smoke117db IF EXISTS WAIT")
        os.unlink(pol.name)


def main() -> None:
    run_cli("targets")
    run_cli("backup", "demo")
    run_cli("verify", "demo")

    # guard: a mutating command without --confirm must refuse (exit 3, no mutation)
    refused = run_cli("restore", "demo", expect_ok=False, expect_exit=Exit.GUARD)
    assert refused["error"]["kind"] == "confirm_required", refused

    # a restricted customer user reads through the alias (#117)
    setup_customer("acme-orders")
    before = _customer_reads("acme-orders")

    # dry-run previews the plan, mutates nothing; it lists the privileges to copy (#117)
    dry = run_cli("restore", "demo", "--dry-run")
    assert dry["result"]["dry_run"] is True and "plan" in dry["result"], dry
    orders = next(m for m in dry["result"]["plan"]["members"] if m["alias"] == "acme-orders")
    assert any("smoke117" in c for c in orders["privileges"]), orders

    run_cli("aggregate", "demo", "--confirm")
    restored = run_cli("restore", "demo", "--confirm", "--ticket", "CHG-SMOKE",
                       "--reason", "cli smoke")
    assert restored["result"]["ticket"] == "CHG-SMOKE", restored  # recorded (#119)
    base = restored["result"]["baseline"]  # a FULL of each new database (#97)
    assert [b["taken"] for b in base] == ["full"] * 3, base
    after = _customer_reads("acme-orders")  # raises 42NFF if the privileges were dropped
    assert after == before, (before, after)
    print(f"== customer user still reads acme-orders after restore ({after} nodes) ==")
    _write("acme-orders", "CREATE (:AfterRestore)")
    diff = run_cli("backup", "demo", "--kind", "DIFF")  # the baseline gives the diff a base
    assert diff["result"]["needs_full"] == [], diff
    assert {b["taken"] for b in diff["result"]["backups"]} == {"diff"}, diff
    print("== after the restore's baseline, DIFF takes a diff ==")
    by_name_privileges()
    run_cli("metadata", "export")
    run_cli("metadata", "restore", "--confirm")
    run_cli("system-backup")

    pruned = run_cli("prune", "--confirm")
    print(f"== prune deleted {pruned['result']['deleted']} artifacts ==")

    print("PASS: neo4j-backup CLI full loop against the stack")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
