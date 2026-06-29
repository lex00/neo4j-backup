#!/usr/bin/env bash
# Adopt an EXISTING database under the alias model so this tooling can back it up and
# restore it. Apps must connect via the alias (not the database name).
#
# A database and an alias cannot share a name, so there are two cases:
#   adopt.sh <database> <alias>             # alias != database: just create the alias (no disruption)
#   adopt.sh <database> <alias> --migrate   # alias == database: back up -> restore into a
#                                           #   uniquely-named physical -> DROP the original
#                                           #   -> create the alias. (Drops the original DB!)
#
# This is the LOCAL helper (wired to the demo stack). In production run the same Cypher
# against your cluster: CREATE ALIAS `<alias>` FOR DATABASE `<database>` (no-clash), or
# the migrate sequence below.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DB="${1:?usage: adopt.sh <database> <alias> [--migrate]}"
ALIAS="${2:?usage: adopt.sh <database> <alias> [--migrate]}"
MODE="${3:-}"

if [ "$DB" != "$ALIAS" ]; then
  echo ">> creating alias '$ALIAS' -> existing database '$DB' (no disruption)"
  sys "CREATE ALIAS \`$ALIAS\` IF NOT EXISTS FOR DATABASE \`$DB\`;"
  echo ">> done. Point apps at alias '$ALIAS'; add it to your policy and back up."
  exit 0
fi

if [ "$MODE" != "--migrate" ]; then
  echo "!! alias name equals database name ('$DB'); they cannot share a name." >&2
  echo "!! Re-run with --migrate to back up '$DB', restore into a new physical, DROP the" >&2
  echo "!! original, and create the alias. This DROPS the original database — make sure" >&2
  echo "!! apps are ready to follow the alias." >&2
  exit 2
fi

ts="$(naming_ts)"; slug="$(naming_slug "$DB")"; newdb="$(naming_physical "$DB" "$ts")"
prefix="adopt/$slug/$newdb"
echo ">> [migrate] backing up '$DB' -> s3://$BUCKET/$prefix/"
$COMPOSE exec -T runner neo4j-admin database backup --from="$BACKUP_SOURCE" \
  --to-path="s3://$BUCKET/$prefix/" --temp-path="$SCRATCH_PATH" \
  --pagecache="$RUNNER_PAGECACHE" --type=AUTO --compress=true "$DB" >/dev/null
artifact="$(latest_artifact "$prefix/")"
echo ">> [migrate] restoring into new physical '$newdb'"
sys "CREATE DATABASE \`$newdb\` OPTIONS { seedURI: 's3://$BUCKET/$prefix/$artifact' } WAIT;"
echo ">> [migrate] dropping original '$DB' and aliasing '$DB' -> '$newdb'"
sys "DROP DATABASE \`$DB\` WAIT;"
sys "CREATE ALIAS \`$DB\` FOR DATABASE \`$newdb\`;"
echo ">> done. Alias '$DB' now resolves to '$newdb'; apps using '$DB' follow it transparently."
