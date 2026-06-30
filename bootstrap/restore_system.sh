#!/usr/bin/env bash
# Offline restore of the `system` database (#15) — the EXACT metadata restore (native
# password hashes, roles, privileges, catalog) that the agentless logical export (#14)
# cannot provide. Companion to `neo4j_system_backup` / the system_backup asset.
#
# `system` cannot be seed-from-URI'd or STOPPed, so this is offline + node-local (path B):
# the DBMS must be DOWN while neo4j-admin rewrites the store. This script demonstrates the
# loop on the local stack; in production the portable core is the single
# `neo4j-admin database restore` invocation, run on the host with the DBMS offline.
#
#   ./bootstrap/restore_system.sh [s3-key]      # default: the latest _dbms/system artifact
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PREFIX="_dbms/system/"   # matches neo4j_backup_core.paths.system_prefix()
NET="neo4j-backup_ops"
DATA_VOL="neo4j-backup_neo4j-data"

# 1. Resolve the artifact key (latest under _dbms/system/, or the one passed in). List
#    non-recursively (`mc ls`, not `mc find`) and select by filename so a stray subdir
#    can't be picked up; system fulls are flat: _dbms/system/system-<ISO>.backup.
KEY="${1:-}"
if [ -z "$KEY" ]; then
  # the mc image is minimal (no grep), so list inside the container and filter on the host
  LS=$($COMPOSE run --rm -T mc \
    "mc alias set local http://minio:9000 $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY >/dev/null && \
     mc ls local/$BUCKET/${PREFIX}" 2>/dev/null)
  NAME=$(printf '%s\n' "$LS" | grep -oE 'system-[^ ]+\.backup' | sort | tail -1)
  [ -n "$NAME" ] && KEY="${PREFIX}${NAME}"
fi
[ -n "$KEY" ] || { echo "no system backup found under $PREFIX — run a system backup first"; exit 1; }
echo ">> restoring system from s3://$BUCKET/$KEY"

# 2. DBMS down — neo4j-admin needs exclusive access to the store.
echo ">> stopping neo4j (offline window)"
$COMPOSE stop neo4j >/dev/null

# 3. Offline restore, reading the artifact straight from object storage (no local copy).
#    --temp-path must be writable (the compressed artifact is unpacked there).
docker run --rm -e NEO4J_ACCEPT_LICENSE_AGREEMENT=eval \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e AWS_REGION="${AWS_REGION:-us-east-1}" -e AWS_ENDPOINT_URL_S3="http://minio:9000" \
  --network "$NET" -v "$DATA_VOL":/data "neo4j:${NEO4J_VERSION}" \
  neo4j-admin database restore --from-path="s3://$BUCKET/$KEY" \
    --temp-path=/tmp --overwrite-destination=true

# 4. DBMS back up.
echo ">> starting neo4j"
$COMPOSE start neo4j >/dev/null
for _ in $(seq 1 30); do
  $COMPOSE exec -T neo4j cypher-shell -u neo4j -p "$PW" "RETURN 1" >/dev/null 2>&1 && break
  sleep 2
done

# 5. Apply the database-permissions script neo4j-admin emits alongside a system restore
#    (restores database access privileges that live outside the system store proper).
META="/data/scripts/system/restore_metadata.cypher"
if $COMPOSE exec -T neo4j test -f "$META"; then
  echo ">> applying $META"
  $COMPOSE exec -T neo4j cypher-shell -u neo4j -p "$PW" \
    --param "database => 'system'" -f "$META" >/dev/null || \
    echo "!! restore_metadata.cypher did not apply cleanly — review manually"
fi

echo ">> system restored. Native users/roles/privileges are back with their EXACT secrets."
echo "   (In production: run the neo4j-admin restore on the host with the DBMS offline.)"
