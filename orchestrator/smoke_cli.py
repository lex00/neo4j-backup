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


def main() -> None:
    run_cli("targets")
    run_cli("backup", "demo")
    run_cli("verify", "demo")

    # guard: a mutating command without --confirm must refuse (exit 3, no mutation)
    refused = run_cli("restore", "demo", expect_ok=False, expect_exit=Exit.GUARD)
    assert refused["error"]["kind"] == "confirm_required", refused

    # dry-run previews the plan, mutates nothing
    dry = run_cli("restore", "demo", "--dry-run")
    assert dry["result"]["dry_run"] is True and "plan" in dry["result"], dry

    run_cli("aggregate", "demo", "--confirm")
    run_cli("restore", "demo", "--confirm")
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
