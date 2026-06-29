#!/usr/bin/env bash
# Restore a group via seed-from-URI into fresh uniquely-named databases, then repoint
# the aliases (group-aligned cutover). Optional PITR. Pure Cypher over Bolt — nothing
# runs on the instance. Non-destructive: old databases are retained for rollback.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

GROUP="${1:-$DEMO_GROUP}"
UNTIL="${2:-}"            # optional ISO-8601, e.g. 2026-06-28T12:00:00Z (applied to all)
ts="$(naming_ts)"
until_clause=""
[ -n "$UNTIL" ] && until_clause=", seedRestoreUntil: datetime('$UNTIL')"

echo ">> restoring group '$GROUP'${UNTIL:+ to $UNTIL} (new physical names @ $ts)"
aliases=(); news=(); olds=()

# Phase 1 — seed each alias's latest backup into a new, uniquely-named physical database.
for raw in "${DEMO_ALIASES[@]}"; do
  alias_name="$(naming_alias "$raw")"
  slug="$(naming_slug "$alias_name")"
  artifact="$(latest_artifact "$GROUP/$slug/")"
  if [ -z "$artifact" ]; then
    echo "!! no .backup under $GROUP/$slug/ — run 'just backup $GROUP' first" >&2; exit 1
  fi
  uri="s3://$BUCKET/$GROUP/$slug/$artifact"
  newdb="$(naming_physical "$alias_name" "$ts")"
  naming_validate_db "$newdb"
  echo ">>   seed $newdb <= $uri"
  # CloudSeedProvider (s3/gs/azb) does NOT accept seedConfig; region/endpoint come from
  # the server's AWS_REGION / AWS_ENDPOINT_URL_S3 env.
  sys "CREATE DATABASE \`$newdb\` OPTIONS { seedURI: '$uri'$until_clause } WAIT;"
  aliases+=("$alias_name"); news+=("$newdb"); olds+=("$(alias_target "$alias_name")")
done

# Phase 2 — repoint all aliases after every seed succeeds (group-aligned cutover).
for i in "${!aliases[@]}"; do
  echo ">>   alias ${aliases[$i]}: ${olds[$i]:-<none>} -> ${news[$i]}"
  sys "ALTER ALIAS \`${aliases[$i]}\` SET DATABASE TARGET \`${news[$i]}\`;"
done

echo ">> cutover complete. In-flight transactions on these aliases were rolled back; apps retry."
echo ">> rollback:  ALTER ALIAS <alias> SET DATABASE TARGET <old-physical>"
echo ">> cleanup after soak:  DROP DATABASE <old-physical>"
