#!/usr/bin/env bash
# Boot the demo from scratch: for each declared alias, create a uniquely-named physical
# database (via the naming authority), load demo data, and point the alias at it.
# All Cypher is sent from the runner over Bolt; nothing runs on the instance.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wait_for_neo4j
ts="$(naming_ts)"
echo ">> creating demo group '$DEMO_GROUP' (alias-fronted; names via bootstrap/naming.sh)"
for raw in "${DEMO_ALIASES[@]}"; do
  alias_name="$(naming_alias "$raw")"
  slug="$(naming_slug "$alias_name")"
  phys="$(naming_physical "$alias_name" "$ts")"
  naming_validate_db "$phys"
  sys "CREATE DATABASE \`$phys\` IF NOT EXISTS WAIT;"
  cyp -d "$phys" < "bootstrap/demo_data/${slug}.cypher"
  sys "CREATE ALIAS \`$alias_name\` IF NOT EXISTS FOR DATABASE \`$phys\`;"
  echo ">>   alias $alias_name -> $phys"
done
echo ">> aliases now:"
sys "SHOW ALIASES FOR DATABASE YIELD name, database;"
echo ">> bootstrap complete. Try: just backup demo"
