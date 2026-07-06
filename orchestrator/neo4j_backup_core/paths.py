"""Storage-key layout: `<group>/<slug>/<physical>/<artifact>.backup` (per-store chains).

A chain lives in one `<physical>/` directory; the `<slug>/` level groups an alias's
physicals so "latest for the alias" is the newest artifact across them.

The scheme is a `PathLayout` object (#21) so a deployment can bring its own bucket
conventions without monkeypatching: implement `PathLayout` (or subclass `DefaultPathLayout`)
and select it with `PATH_LAYOUT=your.module.YourLayout`. Adapters call `get_layout()` once and
inject the instance. The module-level functions remain as thin delegates to the configured
layout, for back-compat and core-internal use.
"""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable

from . import naming


@runtime_checkable
class PathLayout(Protocol):
    def alias_prefix(self, group_id: str, alias: str) -> str: ...
    def physical_prefix(self, group_id: str, alias: str, physical: str) -> str: ...
    def physical_of_key(self, group_id: str, alias: str, key: str) -> str: ...
    def metadata_prefix(self) -> str: ...
    def metadata_key(self, ts: str) -> str: ...
    def system_prefix(self) -> str: ...


class DefaultPathLayout:
    """`<group>/<slug>/<physical>/<artifact>.backup` — the validated per-store chain scheme.

    DBMS-wide artifacts live under the reserved `_dbms/` prefix: the logical metadata export
    (`metadata_key`) and the binary `system`-database backup (`system_prefix`, physical name
    fixed to "system").
    """

    def alias_prefix(self, group_id: str, alias: str) -> str:
        return f"{group_id}/{naming.slug(alias)}/"

    def physical_prefix(self, group_id: str, alias: str, physical: str) -> str:
        return f"{self.alias_prefix(group_id, alias)}{physical}/"

    def physical_of_key(self, group_id: str, alias: str, key: str) -> str:
        return key[len(self.alias_prefix(group_id, alias)):].split("/")[0]

    def metadata_prefix(self) -> str:
        return "_dbms/"

    def metadata_key(self, ts: str) -> str:
        return f"{self.metadata_prefix()}metadata-{ts}.cypher"

    def system_prefix(self) -> str:
        return "_dbms/system/"


def get_layout() -> PathLayout:
    """The configured layout: `DefaultPathLayout`, or the class named by `PATH_LAYOUT`
    (e.g. ``mypkg.CustomLayout``). Adapters call this once and inject the instance."""
    spec = os.environ.get("PATH_LAYOUT")
    if not spec:
        return DefaultPathLayout()
    import importlib

    module, _, cls = spec.rpartition(".")
    if not module:
        raise RuntimeError(f"PATH_LAYOUT must be 'module.Class', got {spec!r}")
    return getattr(importlib.import_module(module), cls)()


# Module-level shims delegate to the configured layout — back-compat and core-internal use
# (e.g. metadata.py). They honor PATH_LAYOUT exactly as get_layout() does.
_DEFAULT: PathLayout = get_layout()


def alias_prefix(group_id: str, alias: str) -> str:
    return _DEFAULT.alias_prefix(group_id, alias)


def physical_prefix(group_id: str, alias: str, physical: str) -> str:
    return _DEFAULT.physical_prefix(group_id, alias, physical)


def physical_of_key(group_id: str, alias: str, key: str) -> str:
    return _DEFAULT.physical_of_key(group_id, alias, key)


def metadata_prefix() -> str:
    return _DEFAULT.metadata_prefix()


def metadata_key(ts: str) -> str:
    return _DEFAULT.metadata_key(ts)


def system_prefix() -> str:
    return _DEFAULT.system_prefix()
