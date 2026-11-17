#!/usr/bin/env bash
# `just try` (#269): the local loop in one command. Resets the local Compose stack, installs the
# CLI, then plays a restore request on the demo group: back up, a customer deletes their orders,
# back up again, restore to just before the delete, and show the undo.
# `just try-mcp` (#280) runs the same story, but the restore goes through the MCP server: the
# restore_request prompt played by bootstrap/try_mcp.py over a real MCP session.
set -euo pipefail
MODE="${1:-cli}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source bootstrap/lib.sh

B=$'\033[1m'; N=$'\033[0m'
step() { printf '\n%s== %s%s\n' "$B" "$*" "$N"; }
say() { printf '   %s\n' "$*"; }
orders() { cyp -d acme-orders --format plain "MATCH (o:Order) RETURN count(o);" | tail -1 | tr -d ' \r'; }

step "1/7 Start a fresh local stack: Neo4j Enterprise, an S3 emulator, a runner with neo4j-admin"
say "This resets the local stack (just down, then just fresh). It takes a few minutes the first time."
just down >/dev/null 2>&1 || true
just fresh 2>&1 | grep -E '^>>   alias|bootstrap complete' || { echo "!! just fresh failed; run it alone to see why" >&2; exit 1; }

step "2/7 Install the CLI"
if [ ! -x orchestrator/.venv/bin/neo4j-backup ]; then
  python3 -m venv orchestrator/.venv
  orchestrator/.venv/bin/pip install -q -e 'orchestrator[dev]'
fi
if [ "$MODE" = mcp ] && ! orchestrator/.venv/bin/python -c 'import mcp' 2>/dev/null; then
  orchestrator/.venv/bin/pip install -q -e 'orchestrator[dev,mcp]'
fi
set -a; . ./.env; set +a
export NEO4J_BOLT_URI=neo4j://localhost:7687 AWS_ENDPOINT_URL_S3=http://localhost:9000 \
       NEO4J_BACKUP_POLICY=policies/demo.yaml BACKUP_UPLOAD=admin \
       RUNNER_EXEC_PREFIX='["docker","compose","--env-file",".env","-f","docker/compose.yaml","exec","-T","runner"]'
LOG=.try.log; : > "$LOG"
# run a CLI command, print it, keep neo4j-admin's output in $LOG, and show a short summary
nb() { local fmt="$1"; shift
  printf '%s$ neo4j-backup %s%s\n' "$B" "$*" "$N"
  orchestrator/.venv/bin/neo4j-backup --json "$@" 2>>"$LOG" | orchestrator/.venv/bin/python bootstrap/try_fmt.py "$fmt"; }
say "neo4j-backup $(orchestrator/.venv/bin/python -c 'import neo4j_backup_core as c; print(c.__version__)')"

step "3/7 Policy targets: group demo, three databases behind aliases"
nb targets targets

step "4/7 Nightly backup (a scheduler runs this in production)"
nb backup backup demo

step "5/7 Delete every Order in acme-orders (the bad change)"
GOOD="$(server_now)"
wait_until_after "$GOOD"
say "orders before: $(orders)    last good moment: $GOOD"
cyp -d acme-orders "MATCH (o:Order) DETACH DELETE o;" >/dev/null
say "orders after the delete: $(orders)"
say "The next scheduled backup captures the delete:"
nb backup backup demo

if [ "$MODE" = mcp ]; then
  step "6/7 Restore request through the MCP server (read-write), as an agent runs it"
  orchestrator/.venv/bin/python bootstrap/try_mcp.py demo "$GOOD" TRY-1
  say "orders now: $(orders)    (the alias points at a new database seeded from the backup)"
  step "7/7 Connect an MCP client"
  say "MCP client config: the environment above, plus"
  say "  command: orchestrator/.venv/bin/neo4j-backup-mcp   (details in MCP.md)"
  say "  prompts: restore_request, check_coverage, onboard, bulk_load, offboard, recover_cluster"
else
  step "6/7 Restore the group to the moment before the delete"
  nb restore-points restore-points demo
  nb preview restore demo --until "$GOOD" --dry-run
  nb restore restore demo --until "$GOOD" --confirm --ticket TRY-1 --reason "orders deleted by mistake"
  say "orders now: $(orders)    (the alias points at a new database seeded from the backup)"

  step "7/7 Rollback preview: the previous databases stay available"
  nb rollback rollback demo --dry-run
fi

printf '\n%sDone.%s Next:\n' "$B" "$N"
say "Look at the backups:        just artifacts      (neo4j-admin output: $LOG)"
say "Write your own policy:      policies/example.yaml and POLICY.md"
say "Run it for real:            GETTING-STARTED.md, part 2"
[ "$MODE" = mcp ] || say "The same restore by agent:  just try-mcp"
