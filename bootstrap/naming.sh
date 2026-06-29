#!/usr/bin/env bash
# Naming authority — the single designated place that owns naming. Nothing else builds
# names by hand. Three distinct identifiers, because Neo4j's alias and database rules
# differ and teams already depend on the alias freedom:
#
#   1. ALIAS    — the team-designated, app-facing name. Validated against Neo4j's FULL
#                 alias spec and PRESERVED EXACTLY (case, underscores, dots, symbols).
#                 We never force a team to rename an existing alias.
#   2. SLUG     — a deterministic, db-legal, path-safe id derived from the alias. Used
#                 for object-storage prefixes and as the base of physical db names.
#                 Clean aliases pass through unchanged; messy ones get sanitized + a
#                 short stable hash so distinct aliases never collide.
#   3. PHYSICAL — <slug><join><ts>: the unique standard-database name a restore seeds
#                 into. Always a legal Neo4j database name.
#
# Neo4j rules enforced here:
#   database name: [a-z0-9.-], 3-63, lowercase, start/end alphanumeric, NO underscores,
#                  reserved `_*`/`system*`.
#   alias name:    up to 65534 chars, almost any character (backtick-quoted in Cypher),
#                  cannot end with a dot, reserved `_*`/`system*`. A dot is parsed as a
#                  composite database delimiter (database.constituent) — allowed, flagged.
#
# Override the timestamp format or join character in ONE place here. This module mirrors
# the orchestrator's NamingPolicy (next layer) so shell and Python designate identically.
set -euo pipefail

NAMING_TS_FMT="${NAMING_TS_FMT:-%Y%m%dt%H%M%S}"   # dashless on purpose (see parse fn)
NAMING_JOIN="-"                                    # separator between slug and ts
NAMING_MAXLEN=63                                    # database name ceiling
NAMING_ALIAS_MAXLEN=65534                           # alias name ceiling
NAMING_SLUG_BASE_MAXLEN=40                          # slug base before hash/ts headroom

# A name-safe UTC timestamp, e.g. 20260628t120000. Contains no NAMING_JOIN char.
naming_ts() { date -u "+$NAMING_TS_FMT"; }

_naming_hash() {
  if command -v shasum >/dev/null 2>&1; then printf '%s' "$1" | shasum | cut -c1-8
  else printf '%s' "$1" | sha1sum | cut -c1-8; fi
}

# Coerce an arbitrary string into a db-legal fragment: lowercase, illegal runs -> '-',
# trimmed of leading/trailing '.' and '-'.
naming_sanitize() {
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9.-]+/-/g; s/^[.-]+//; s/[.-]+$//'
}

# Assert a legal Neo4j DATABASE name (strict). Non-zero + message if illegal.
naming_validate_db() {
  local n="$1"
  if ! printf '%s' "$n" | grep -Eq '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'; then
    echo "!! illegal database name '$n' (need [a-z0-9.-], 3-63 chars, start/end alnum)" >&2
    return 1
  fi
  case "$n" in _*|system*) echo "!! reserved database name '$n'" >&2; return 1 ;; esac
}

# Assert a legal Neo4j ALIAS name (permissive — the FULL set). Preserves the name;
# only rejects what Neo4j actually forbids. Emits non-fatal notes for backtick/composite.
naming_validate_alias() {
  local a="$1"
  if [ -z "$a" ]; then echo "!! empty alias name" >&2; return 1; fi
  if (( ${#a} > NAMING_ALIAS_MAXLEN )); then
    echo "!! alias '$a' exceeds $NAMING_ALIAS_MAXLEN chars" >&2; return 1
  fi
  case "$a" in
    _*|system*) echo "!! reserved alias name '$a' (no '_'/'system' prefix)" >&2; return 1 ;;
  esac
  case "$a" in
    *.) echo "!! alias '$a' cannot end with a dot" >&2; return 1 ;;
  esac
  case "$a" in
    *.*) echo ">> note: alias '$a' contains a dot — Neo4j parses it as a composite" \
              "database delimiter (database.constituent)" >&2 ;;
  esac
  if ! printf '%s' "$a" | grep -Eq '^[a-zA-Z0-9-]+$'; then
    echo ">> note: alias '$a' must be backtick-quoted in Cypher" >&2
  fi
  return 0
}

# The app-facing alias: validate against the full spec, return it UNCHANGED.
naming_alias() { naming_validate_alias "$1" || return 1; printf '%s' "$1"; }

# Deterministic, db-legal, path-safe slug for an alias. Clean aliases pass through;
# messy ones become sanitize(alias) truncated + '-' + 8-char hash(alias) so distinct
# aliases never collide after sanitization.
naming_slug() {
  local a="$1" base
  # already clean, short, db-legal (no dots to keep paths simple) -> use as-is
  if printf '%s' "$a" | grep -Eq '^[a-z0-9][a-z0-9-]{1,'"$NAMING_SLUG_BASE_MAXLEN"'}[a-z0-9]$'; then
    printf '%s' "$a"; return 0
  fi
  base="$(naming_sanitize "$a")"
  base="${base:0:$NAMING_SLUG_BASE_MAXLEN}"
  base="$(printf '%s' "$base" | sed -E 's/[.-]+$//')"
  [ -z "$base" ] && base="db"
  printf '%s-%s' "$base" "$(_naming_hash "$a")"
}

# Unique physical database name for an alias at a timestamp: <slug><join><ts>.
naming_physical() {
  local slug ts tslen
  slug="$(naming_slug "$1")"
  ts="${2:-$(naming_ts)}"
  tslen=$(( ${#ts} + ${#NAMING_JOIN} ))
  if (( ${#slug} + tslen > NAMING_MAXLEN )); then
    slug="${slug:0:$(( NAMING_MAXLEN - tslen ))}"
    slug="$(printf '%s' "$slug" | sed -E 's/[.-]+$//')"
  fi
  printf '%s%s%s' "$slug" "$NAMING_JOIN" "$ts"
}

# Recover the slug from a physical name (strip trailing <join><ts>).
naming_slug_of_physical() { printf '%s' "${1%"$NAMING_JOIN"*}"; }
