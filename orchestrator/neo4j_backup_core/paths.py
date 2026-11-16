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
    def metadata_prefix(self, cluster: str = "default") -> str: ...
    def metadata_key(self, ts: str, cluster: str = "default") -> str: ...
    def metadata_listing(self, cluster: str = "default") -> str: ...
    def system_prefix(self, cluster: str = "default") -> str: ...
    def load_prefix(self, group_id: str, alias: str) -> str: ...


class DefaultPathLayout:
    """`<group>/<slug>/<physical>/<artifact>.backup` — the validated per-store chain scheme.

    DBMS-wide artifacts live under the reserved `_dbms/` prefix: the logical metadata export
    (`metadata_key`) and the binary `system`-database backup (`system_prefix`, physical name
    fixed to "system").
    """

    # (group_id, alias) -> a member's `overrides.<alias>.prefix`, or None (#90). get_layout()
    # sets it from the policy; a custom layout gets the same attribute.
    prefix_for = None

    def alias_prefix(self, group_id: str, alias: str) -> str:
        pinned = self.prefix_for(group_id, alias) if self.prefix_for else None
        return pinned or f"{group_id}/{naming.scheme().slug(alias)}/"  # (#89)

    def physical_prefix(self, group_id: str, alias: str, physical: str) -> str:
        return f"{self.alias_prefix(group_id, alias)}{physical}/"

    def physical_of_key(self, group_id: str, alias: str, key: str) -> str:
        return key[len(self.alias_prefix(group_id, alias)):].split("/")[0]

    def load_prefix(self, group_id: str, alias: str) -> str:
        """Load artifacts for one member (#156): `.dump` (or `.backup`) files a load or seed
        uses, beside its physical folders. `_seed` can't be a database name (no underscore)."""
        return f"{self.alias_prefix(group_id, alias)}_seed/"

    # DBMS objects per cluster (#106): the default cluster keeps the original keys
    # (`_dbms/metadata-<ts>.cypher`, `_dbms/system/`); a named cluster uses `_dbms/<cluster>/`.
    # Listings use exact prefixes, so clusters never see each other's objects (the cluster names
    # `system` and `metadata…` are reserved by the policy).
    def metadata_prefix(self, cluster: str = "default") -> str:
        return "_dbms/" if cluster == "default" else f"_dbms/{cluster}/"

    def metadata_key(self, ts: str, cluster: str = "default") -> str:
        return f"{self.metadata_prefix(cluster)}metadata-{ts}.cypher"

    def metadata_listing(self, cluster: str = "default") -> str:
        return f"{self.metadata_prefix(cluster)}metadata-"

    def system_prefix(self, cluster: str = "default") -> str:
        return "_dbms/system/" if cluster == "default" else f"_dbms/{cluster}/system/"


def _member_prefix(group, alias: str) -> str | None:
    """A member's folder when the policy decides it: `overrides.<alias>.prefix` (#90), else the
    readable slug when the group's `naming.slug_style` is `readable` (#91); else None."""
    ov = (group.overrides or {}).get(alias)
    if isinstance(ov, dict) and ov.get("prefix"):
        return ov["prefix"]
    if naming.readable(group) and not os.environ.get("NAMING"):
        return f"{group.id}/{naming.slug_readable(alias)}/"
    return None


def prefixes_of(policy) -> dict[tuple[str, str], str]:
    """{(group, alias): prefix} for every member whose folder the policy decides (#90, #91)."""
    return {(g.id, a): p for g in policy.db_groups for a in g.names
            if (p := _member_prefix(g, a))}


def prefix_from(policy):
    """A `prefix_for` callable over a fixed policy (tests, one-off callers)."""
    pinned = prefixes_of(policy)
    return lambda group_id, alias: pinned.get((group_id, alias))


def _policy_prefix(group_id: str, alias: str) -> str | None:
    """`prefix_for` over the configured policy, read through load_policy's cache (#90). No
    loadable policy: no overrides."""
    try:
        from .policy import load_policy

        pol = load_policy(os.environ.get("NEO4J_BACKUP_POLICY", "policies/demo.yaml"))
    except Exception:  # noqa: BLE001 - no policy (bootstrap, a probe): the default layout
        return None
    try:
        return _member_prefix(pol.group(group_id), alias)
    except KeyError:
        return None


def get_layout() -> PathLayout:
    """The configured layout: `DefaultPathLayout`, or the class named by `PATH_LAYOUT`
    (e.g. ``mypkg.CustomLayout``). Adapters call this once and inject the instance. Either way
    the layout's `prefix_for` gives members' `overrides.<alias>.prefix` from the policy (#90)."""
    spec = os.environ.get("PATH_LAYOUT")
    if not spec:
        layout = DefaultPathLayout()
    else:
        import importlib

        module, _, cls = spec.rpartition(".")
        if not module:
            raise RuntimeError(f"PATH_LAYOUT must be 'module.Class', got {spec!r}")
        layout = getattr(importlib.import_module(module), cls)()
    try:
        layout.prefix_for = _policy_prefix
    except AttributeError:  # a layout with __slots__: it doesn't take overrides
        pass
    return layout


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


def load_prefix(group_id: str, alias: str) -> str:
    return _DEFAULT.load_prefix(group_id, alias)


def metadata_listing(cluster: str = "default") -> str:
    return _DEFAULT.metadata_listing(cluster)


def metadata_key(ts: str) -> str:
    return _DEFAULT.metadata_key(ts)


def system_prefix() -> str:
    return _DEFAULT.system_prefix()
