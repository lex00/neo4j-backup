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
