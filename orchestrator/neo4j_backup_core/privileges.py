"""Database-scoped privileges across a physical swap (#117).

Neo4j stores a privilege against the physical database, even when it was granted through an alias:
`GRANT MATCH {*} ON GRAPH orders TO r` is saved as ``GRANT MATCH {*} ON GRAPH `orders-2026…` NODE *
TO `r` ``. Moving the alias to a new physical therefore leaves every customer role without access,
and DROP DATABASE removes the privileges outright (verified on 2026.05; see #117). So before a swap
the privileges on the old physical are re-granted on the new one, and a by-name restore captures
them before its drop and re-grants them after the create.

`SHOW PRIVILEGES AS COMMANDS` returns one command per graph, with names backtick-quoted. Privileges
on `*`, `HOME` or `DEFAULT` never name the database and are left alone.
"""

from __future__ import annotations

import re

from .cypher import quote  # the one quoting helper (#77)


def _on(database: str) -> re.Pattern:
    # ` ON GRAPH `db`` or ` ON DATABASE `db``, not followed by another backtick (so `a` does not
    # match the quoted name `a``b`).
    return re.compile(r" ON (GRAPH|DATABASE) " + re.escape(quote(database)) + r"(?!`)")


def scoped_to(commands: list[str], database: str) -> list[str]:
    """The commands whose ON clause names `database`."""
    pat = _on(database)
    return [c for c in commands if pat.search(c)]


def rewrite(command: str, old: str, new: str) -> str:
    """`command` with its ON clause moved from `old` to `new`; role names are not touched."""
    return _on(old).sub(lambda m: f" ON {m.group(1)} {quote(new)}", command)
