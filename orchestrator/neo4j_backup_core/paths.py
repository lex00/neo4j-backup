"""Storage-key layout: `<group>/<slug>/<physical>/<artifact>.backup` (per-store chains).

A chain lives in one `<physical>/` directory; the `<slug>/` level groups an alias's
physicals so "latest for the alias" is the newest artifact across them.
"""

from . import naming


def alias_prefix(group_id: str, alias: str) -> str:
    return f"{group_id}/{naming.slug(alias)}/"


def physical_prefix(group_id: str, alias: str, physical: str) -> str:
    return f"{alias_prefix(group_id, alias)}{physical}/"


def physical_of_key(group_id: str, alias: str, key: str) -> str:
    return key[len(alias_prefix(group_id, alias)):].split("/")[0]


# DBMS-wide logical metadata export (users/roles/privileges/aliases) — not per-group, so
# it lives under a reserved prefix alongside the per-group backup trees.
def metadata_prefix() -> str:
    return "_dbms/"


def metadata_key(ts: str) -> str:
    return f"{metadata_prefix()}metadata-{ts}.cypher"


# Binary `system`-database backup (#15) — exact metadata restore (native passwords) via the
# offline node-local path. DBMS-wide, so it sits under the reserved prefix; system's physical
# name is fixed ("system"), so this is the chain directory.
def system_prefix() -> str:
    return "_dbms/system/"
