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


def ts_format() -> str:
    """The timestamp format: NAMING_TS_FMT, as in naming.sh (#86), else TS_FMT. A format
    containing the join character would make names ambiguous, so it is refused."""
    import os

    fmt = os.environ.get("NAMING_TS_FMT") or TS_FMT
    if JOIN in fmt:
        raise NamingError(f"NAMING_TS_FMT {fmt!r} contains {JOIN!r}, the join character")
    return fmt

# Neo4j database names (Operations Manual, "Database names", 5.x and 2025.x): start with an
# ASCII letter, then letters, digits, dots and dashes, 3-63 characters; `_` and `system` prefixes
# are reserved. Names are case-insensitive and stored lowercase.
# https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/naming-databases/
_DB_RE = re.compile(r"[a-z][a-z0-9.-]{2,62}")
_SLUG_CLEAN_RE = re.compile(rf"[a-z0-9][a-z0-9-]{{1,{SLUG_BASE_MAXLEN}}}[a-z0-9]")
_ALIAS_PLAIN_RE = re.compile(r"[a-zA-Z0-9-]+")


class NamingError(ValueError):
    """Raised when a name violates the relevant Neo4j rule set."""


def ts(now: datetime | None = None) -> str:
    """Name-safe UTC timestamp, e.g. 20260628t120000."""
    return (now or datetime.now(timezone.utc)).strftime(ts_format())


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
            f"illegal database name {name!r} (need a leading letter, then [a-z0-9.-], 3-63)"
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


def _hashed(a: str) -> bool:
    return not _SLUG_CLEAN_RE.fullmatch(a)


def physical(a: str, ts_val: str | None = None) -> str:
    """Unique physical database name for an alias at a timestamp: <slug>-<ts>. When that is too
    long, the readable base is cut and the 8-character hash kept (#87), so the name still says
    whose it is (`owns`)."""
    s = slug(a)
    if not s[:1].isalpha():  # a database name starts with a letter (#85); the slug is unchanged
        s = f"db{JOIN}{s}"
    t = ts_val if ts_val is not None else ts()
    tslen = len(t) + len(JOIN)
    if len(s) + tslen > DB_MAXLEN:
        if _hashed(a):
            base, h = s.rsplit(JOIN, 1)
            base = re.sub(r"[.-]+$", "", base[: DB_MAXLEN - tslen - len(h) - len(JOIN)])
            s = f"{base or 'db'}{JOIN}{h}"
        else:
            s = re.sub(r"[.-]+$", "", s[: DB_MAXLEN - tslen])
    return validate_db(f"{s}{JOIN}{t}")


def owns(a: str, name: str) -> bool:
    """Whether `name` is a physical this tool would have created for alias `a` (#87, #89):
    `<slug or its cut-down form>-<ts>`; for a hashed slug the alias hash must be intact."""
    prefix, sep, t = name.rpartition(JOIN)
    if not sep or not t or (ts_format() == TS_FMT and not re.fullmatch(r"\d{8}t\d{6}", t)):
        return False
    s = slug(a)
    if not s[:1].isalpha():
        s = f"db{JOIN}{s}"
    if not _hashed(a):
        return prefix == s
    base, h = s.rsplit(JOIN, 1)
    pbase, psep, ph = prefix.rpartition(JOIN)
    return bool(psep) and ph == h and base.startswith(pbase)


def slug_of_physical(name: str) -> str:
    """Recover the slug from a physical name (strip trailing <join><ts>)."""
    return name.rsplit(JOIN, 1)[0]


# --- the naming hook (#89) ------------------------------------------------------------------------

class DefaultNaming:
    """The built-in scheme: `slug`, `physical` and `owns` above. A custom scheme (`NAMING=
    module.Class`) implements the same three methods; `physical` also gets the alias's current
    target, so a scheme can flip between two names (blue/green)."""

    def slug(self, alias: str) -> str:
        return slug(alias)

    def physical(self, alias: str, ts_val: str, current: str | None = None) -> str:
        return physical(alias, ts_val)

    def owns(self, alias: str, name: str) -> bool:
        return owns(alias, name)


class _Validated:
    """Wraps a scheme so every physical name it returns is checked against Neo4j's rules."""

    def __init__(self, inner):
        self._inner = inner

    def slug(self, alias: str) -> str:
        return self._inner.slug(alias)

    def physical(self, alias: str, ts_val: str, current: str | None = None) -> str:
        return validate_db(self._inner.physical(alias, ts_val, current))

    def owns(self, alias: str, name: str) -> bool:
        return bool(self._inner.owns(alias, name))


_SCHEME = None


def scheme():
    """The configured naming scheme, loaded once: `NAMING=module.Class`, else DefaultNaming."""
    global _SCHEME
    if _SCHEME is None:
        import importlib
        import os

        spec = os.environ.get("NAMING")
        if spec:
            module, _, cls = spec.rpartition(".")
            if not module:
                raise NamingError(f"NAMING must be 'module.Class', got {spec!r}")
            inner = getattr(importlib.import_module(module), cls)()
        else:
            inner = DefaultNaming()
        _SCHEME = _Validated(inner)
    return _SCHEME


def reset_scheme() -> None:
    """Forget the loaded scheme (tests)."""
    global _SCHEME
    _SCHEME = None


# --- the policy's `naming:` block (#88) -----------------------------------------------------------

DEFAULT_TEMPLATE = "{slug}-{ts}"
TEMPLATE_FIELDS = ("alias", "slug", "group", "ts")


def check_template(template: str) -> None:
    """A physical_template must contain `{ts}` (each restore needs a new name) and use only
    TEMPLATE_FIELDS."""
    import string

    fields = [f for _lit, f, _spec, _conv in string.Formatter().parse(template) if f is not None]
    unknown = [f for f in fields if f not in TEMPLATE_FIELDS]
    if unknown:
        raise NamingError(f"physical_template {template!r}: unknown field(s) {unknown} "
                          f"(fields: {', '.join('{' + f + '}' for f in TEMPLATE_FIELDS)})")
    if "ts" not in fields:
        raise NamingError(f"physical_template {template!r} must contain {{ts}}, so every "
                          f"restore gets a new database name")


def check_ts_format(fmt: str) -> None:
    if JOIN in fmt:
        raise NamingError(f"ts_format {fmt!r} contains {JOIN!r}, the join character")


def render(template: str, alias: str, group_id: str, ts_val: str) -> str:
    """A physical name from a template; the default template is `physical()` itself, so
    today's names are unchanged."""
    if template == DEFAULT_TEMPLATE:
        return physical(alias, ts_val)
    name = template.format(alias=sanitize(alias) or "db", slug=slug(alias),
                           group=sanitize(group_id) or "g", ts=ts_val)
    return validate_db(name)


class TemplateNaming:
    """The scheme a group's `naming:` block describes (#88)."""

    def __init__(self, group_id: str, template: str):
        self.group_id, self.template = group_id, template

    def slug(self, alias: str) -> str:
        return slug(alias)

    def physical(self, alias: str, ts_val: str, current: str | None = None) -> str:
        return render(self.template, alias, self.group_id, ts_val)

    def owns(self, alias: str, name: str) -> bool:
        if self.template == DEFAULT_TEMPLATE:  # with a custom ts_format: rebuild and compare
            t = name.rpartition(JOIN)[2]
            try:
                return bool(t) and physical(alias, t) == name
            except NamingError:
                return False
        pattern = re.escape(self.template)
        for f, v in (("alias", sanitize(alias) or "db"), ("slug", slug(alias)),
                     ("group", sanitize(self.group_id) or "g")):
            pattern = pattern.replace(re.escape("{" + f + "}"), re.escape(v))
        pattern = pattern.replace(re.escape("{ts}"), r"[a-z0-9.]+")
        return re.fullmatch(pattern, name) is not None


def _group_naming(group):
    n = getattr(group, "naming", None)
    return (getattr(n, "physical_template", None) or DEFAULT_TEMPLATE,
            getattr(n, "ts_format", None))


def for_group(group):
    """The scheme for a group's physical names: a `NAMING` hook (#89) wins; else the group's
    `naming:` block (#88); else the default."""
    import os

    template, fmt = _group_naming(group)
    if os.environ.get("NAMING") or (template == DEFAULT_TEMPLATE and not fmt):
        return scheme()
    return _Validated(TemplateNaming(group.id, template))


def ts_for(group, now: datetime | None = None) -> str:
    """The timestamp for a group's new physical names: its `naming.ts_format`, else
    NAMING_TS_FMT, else the default."""
    _template, fmt = _group_naming(group)
    if not fmt:
        return ts(now)
    check_ts_format(fmt)
    return (now or datetime.now(timezone.utc)).strftime(fmt)
