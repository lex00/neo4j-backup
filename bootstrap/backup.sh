#!/usr/bin/env bash
# Policy-driven backup: resolve each alias to its current physical database and back it
# up to object storage under the alias's slug prefix. neo4j-admin runs on the runner
# (the only CLI step); SSE-KMS encrypts at rest transparently.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

GROUP="${1:-$DEMO_GROUP}"
echo ">> backing up group '$GROUP' from $BACKUP_SOURCE"
for raw in "${DEMO_ALIASES[@]}"; do
  alias_name="$(naming_alias "$raw")"
  slug="$(naming_slug "$alias_name")"
  phys="$(alias_target "$alias_name")"
  if [ -z "$phys" ]; then
    echo "!! alias '$alias_name' has no target — run 'just bootstrap'" >&2; exit 1
  fi
  echo ">>   $alias_name (-> $phys) -> s3://$BUCKET/$GROUP/$slug/"
  # HEAP_SIZE is set on the runner service env. --pagecache MUST be explicit or the
  # backup inherits the server pagecache (60%+ RAM) and OOMs. --temp-path stages on the
  # sized scratch volume, never the install partition.
  $COMPOSE exec -T runner neo4j-admin database backup \
    --from="$BACKUP_SOURCE" \
    --to-path="s3://$BUCKET/$GROUP/$slug/" \
    --temp-path="$SCRATCH_PATH" \
    --pagecache="$RUNNER_PAGECACHE" \
    --type=AUTO --compress=true \
    "$phys"
done
echo ">> done. Inspect: just artifacts $GROUP"
