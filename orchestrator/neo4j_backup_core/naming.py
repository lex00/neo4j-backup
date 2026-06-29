"""Naming authority — Python port of bootstrap/naming.sh.

Three identifiers, the same contract as the shell:
  - alias    : team-designated, validated against Neo4j's FULL alias spec, preserved.
  - slug     : deterministic, db-legal, path-safe id derived from the alias.
  - physical : <slug>-<ts>, the unique standard database a restore seeds into.

Kept byte-for-byte compatible with naming.sh (see tests/test_naming_parity.py).
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

JOIN = "-"
DB_MAXLEN = 63
ALIAS_MAXLEN = 65534
SLUG_BASE_MAXLEN = 40
TS_FMT = "%Y%m%dt%H%M%S"  # dashless, so slug_of_physical can split on JOIN

_DB_RE = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_SLUG_CLEAN_RE = re.compile(rf"[a-z0-9][a-z0-9-]{{1,{SLUG_BASE_MAXLEN}}}[a-z0-9]")
_ALIAS_PLAIN_RE = re.compile(r"[a-zA-Z0-9-]+")


class NamingError(ValueError):
    """Raised when a name violates the relevant Neo4j rule set."""


def ts(now: datetime | None = None) -> str:
    """Name-safe UTC timestamp, e.g. 20260628t120000."""
    return (now or datetime.now(timezone.utc)).strftime(TS_FMT)


def _hash8(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()[:8]


def sanitize(s: str) -> str:
    """Coerce to a db-legal fragment: lowercase, illegal runs -> '-', trimmed."""
    s = s.lower()
    s = re.sub(r"[^a-z0-9.-]+", "-", s)
    s = re.sub(r"^[.-]+", "", s)
    s = re.sub(r"[.-]+$", "", s)
    return s


def validate_db(name: str) -> str:
    """Assert a legal Neo4j DATABASE name (strict). Raises NamingError otherwise."""
    if not _DB_RE.fullmatch(name):
        raise NamingError(
            f"illegal database name {name!r} (need [a-z0-9.-], 3-63, start/end alnum)"
        )
    if name.startswith("_") or name.startswith("system"):
        raise NamingError(f"reserved database name {name!r}")
    return name


def validate_alias(name: str) -> str:
    """Assert a legal Neo4j ALIAS name (the full permissive set). Preserves the name.

    Rejects only what Neo4j forbids; dot (composite delimiter) and backtick-needing
    characters are allowed (callers may inspect needs_backticks/is_composite).
    """
    if not name:
        raise NamingError("empty alias name")
    if len(name) > ALIAS_MAXLEN:
        raise NamingError(f"alias {name!r} exceeds {ALIAS_MAXLEN} chars")
    if name.startswith("_") or name.startswith("system"):
        raise NamingError(f"reserved alias name {name!r} (no '_'/'system' prefix)")
    if name.endswith("."):
        raise NamingError(f"alias {name!r} cannot end with a dot")
    return name


def is_composite(alias: str) -> bool:
    """A dot marks a composite database.constituent reference."""
    return "." in alias


def needs_backticks(alias: str) -> bool:
    return not _ALIAS_PLAIN_RE.fullmatch(alias)


def alias(name: str) -> str:
    """The app-facing alias: validate against the full spec, return UNCHANGED."""
    return validate_alias(name)


def slug(a: str) -> str:
    """Deterministic, db-legal, path-safe slug for an alias.

    Clean aliases pass through; messy ones become sanitize(alias) truncated + an
    8-char hash so distinct aliases never collide after sanitization.
    """
    if _SLUG_CLEAN_RE.fullmatch(a):
        return a
    base = sanitize(a)[:SLUG_BASE_MAXLEN]
    base = re.sub(r"[.-]+$", "", base)
    if not base:
        base = "db"
    return f"{base}-{_hash8(a)}"


def physical(a: str, ts_val: str | None = None) -> str:
    """Unique physical database name for an alias at a timestamp: <slug>-<ts>."""
    s = slug(a)
    t = ts_val if ts_val is not None else ts()
    tslen = len(t) + len(JOIN)
    if len(s) + tslen > DB_MAXLEN:
        s = s[: DB_MAXLEN - tslen]
        s = re.sub(r"[.-]+$", "", s)
    return validate_db(f"{s}{JOIN}{t}")


def slug_of_physical(name: str) -> str:
    """Recover the slug from a physical name (strip trailing <join><ts>)."""
    return name.rsplit(JOIN, 1)[0]
