#!/usr/bin/env bash
# Demonstrate point-in-time recovery. Build a full -> change -> differential chain on
# one database, then restore to a timestamp BEFORE the change (state A) and to HEAD
# (state B), proving seedRestoreUntil replays the chain to a point in time.
#
# PITR requires a chain: a lone full errors ("can only be fully restored"). That is the
# whole reason this demo takes a differential.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DB="pitr-demo"
PREFIX="pitr/$DB"

drop() { sys "DROP DATABASE \`$1\` IF EXISTS;" >/dev/null 2>&1 || true; }

bkp() {  # one backup of $DB into the chain prefix; print the key lines
  $COMPOSE exec -T runner neo4j-admin database backup \
    --from="$BACKUP_SOURCE" --to-path="s3://$BUCKET/$PREFIX/" \
    --temp-path="$SCRATCH_PATH" --pagecache="$RUNNER_PAGECACHE" \
    --type=AUTO --compress=true "$DB" 2>&1 \
    | grep -iE 'differential|full backup|Falling back|completed' || true
}

echo ">> [setup] fresh databases"
drop "$DB"; drop "pitr-at-t0"; drop "pitr-head"
sys "CREATE DATABASE \`$DB\` WAIT;" >/dev/null

echo ">> [state A] add 2 customers (Ada, Alan)"
cyp -d "$DB" "CREATE (:Customer {id:'C1',name:'Ada'}),(:Customer {id:'C2',name:'Alan'});" >/dev/null

echo ">> [backup] FULL -> s3://$BUCKET/$PREFIX/"
bkp

sleep 2
T0="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ">> [mark] T0 = $T0   (state A is before T0; state B will be after)"
sleep 2

echo ">> [state B] add a 3rd customer (Grace) AFTER T0"
cyp -d "$DB" "CREATE (:Customer {id:'C3',name:'Grace'});" >/dev/null

echo ">> [backup] DIFFERENTIAL -> same prefix (forms the chain)"
bkp

artifact="$(latest_artifact "$PREFIX/")"
uri="s3://$BUCKET/$PREFIX/$artifact"
echo ">> chain head artifact: $artifact"

echo ">> [restore @T0]  seedRestoreUntil=$T0  -> expect 2 (state A)"
sys "CREATE DATABASE \`pitr-at-t0\` OPTIONS { seedURI:'$uri', seedRestoreUntil: datetime('$T0') } WAIT;" >/dev/null

echo ">> [restore HEAD] no predicate                -> expect 3 (state B)"
sys "CREATE DATABASE \`pitr-head\` OPTIONS { seedURI:'$uri' } WAIT;" >/dev/null

a="$(cyp -d pitr-at-t0 --format plain 'MATCH (c:Customer) RETURN count(c);' | tail -1 | tr -d ' \r')"
b="$(cyp -d pitr-head  --format plain 'MATCH (c:Customer) RETURN count(c);' | tail -1 | tr -d ' \r')"
echo ">> ===== RESULT ====="
echo ">>   pitr-at-t0 (PITR to $T0): $a customers"
echo ">>   pitr-head  (latest):       $b customers"
if [ "$a" = "2" ] && [ "$b" = "3" ]; then
  echo ">> PASS: point-in-time restore replayed the chain to T0 (2), HEAD is 3."
else
  echo "!! UNEXPECTED: expected 2 and 3, got $a and $b" >&2; exit 1
fi
