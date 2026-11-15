"""Safe pieces for the Cypher statements the tool builds as text (#77).

DDL (CREATE/DROP/STOP DATABASE, CREATE/ALTER ALIAS, GRANT) can't take names as parameters, so
names are backtick-quoted with `quote`, and the one string literal the tool writes, the seed URI,
is checked with `seed_uri_literal`.
"""

from __future__ import annotations

SEED_SCHEMES = ("s3://", "gs://", "azb://", "file:")


def quote(name: str) -> str:
    """Backtick-quote a Neo4j identifier, doubling any backtick inside it."""
    return "`" + str(name).replace("`", "``") + "`"


def seed_uri_literal(uri: str) -> str:
    """`uri` as the inside of a single-quoted Cypher string, after checking it is a seed URI
    Neo4j can read (s3, gs, azb, file) with no quote, backslash or control character in it."""
    if not isinstance(uri, str) or not uri.startswith(SEED_SCHEMES):
        raise ValueError(f"seed URI {uri!r}: expected one of {', '.join(SEED_SCHEMES)}")
    if any(c in uri for c in "'\\") or any(ord(c) < 32 for c in uri):
        raise ValueError(f"seed URI {uri!r} contains a quote, backslash or control character")
    return uri
