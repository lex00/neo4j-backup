#!/usr/bin/env bash
# Shared helpers for the demo scripts. Single-node stack.
#
# Execution surface mirrors production: ALL Cypher is sent from the RUNNER as an
# external Bolt client (the stand-in for Dagster's Neo4j driver) — nothing runs on the
# Neo4j instance. Backups run neo4j-admin on the same runner. The DB instance is
# agentless: it only serves Bolt (7687) and the backup port (6362). There is no Cypher
# API for backup, so backup is the one CLI step; restore is pure Cypher.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a
# naming authority (single source of truth for names)
# shellcheck disable=SC1091
source "$ROOT/bootstrap/naming.sh"

COMPOSE="docker compose --env-file .env -f docker/compose.yaml"
PW="${NEO4J_PASSWORD:-devpassword}"
BUCKET="${BACKUP_BUCKET:-neo4j-backups}"
SEEDCFG="${SEED_CONFIG:-region=us-east-1}"
BACKUP_SOURCE="${NEO4J_BACKUP_SOURCE:-neo4j:6362}"
RUNNER_BOLT="${RUNNER_BOLT:-neo4j://neo4j:7687}"
SCRATCH_PATH="${SCRATCH_PATH:-/scratch}"
RUNNER_PAGECACHE="${RUNNER_PAGECACHE:-512M}"

DEMO_GROUP="demo"
DEMO_ALIASES=(acme-orders acme-graph acme-audit)

# cypher-shell as an EXTERNAL Bolt client from the runner (not exec'd on the instance)
cyp() { $COMPOSE exec -T runner cypher-shell -a "$RUNNER_BOLT" -u neo4j -p "$PW" "$@"; }
sys() { cyp -d system "$@"; }

wait_for_neo4j() {
  echo ">> waiting for neo4j (via runner Bolt)..."
  for _ in $(seq 1 40); do
    if cyp "RETURN 1" >/dev/null 2>&1; then echo ">> neo4j ready"; return 0; fi
    sleep 3
  done
  echo "!! neo4j did not become ready" >&2; return 1
}

# The physical database an alias currently targets.
alias_target() {
  local a="$1"
  cyp -d system --format plain \
    "SHOW ALIASES FOR DATABASE YIELD name, database WHERE name = '$a' RETURN database;" \
    2>/dev/null | tail -n +2 | tr -d '"\r' | head -1
}

# Newest .backup artifact under a bucket prefix. The mc image is minimal (no awk/grep),
# so mc only lists; parsing happens on the host.
latest_artifact() {
  local prefix="$1"
  $COMPOSE run --rm -T mc \
    "mc alias set local http://minio:9000 '$AWS_ACCESS_KEY_ID' '$AWS_SECRET_ACCESS_KEY' >/dev/null 2>&1; \
     mc find \"local/$BUCKET/$prefix\" --name '*.backup'" \
    2>/dev/null | tr -d '\r' | grep '\.backup$' | sed 's#.*/##' | sort | tail -1
}
