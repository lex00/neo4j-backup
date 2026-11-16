"""Pluggable restore cutover (#17).

After a fresh physical is seeded and verified, "cutover" points application traffic at it.
The default is the Neo4j **alias swap** (DESIGN.md §7). A deployment that routes apps through
an **external layer** (proxy / service discovery / DNS / router) overrides with a hook that
repoints that layer, leaving the Neo4j alias untouched.

Select with `CUTOVER_STRATEGY=alias-swap` (default) `| external`; the external hook is
`CUTOVER_HOOK` (an http(s) URL, POSTed the JSON payload, or a shell command receiving the
values as `CUTOVER_*` env vars). No third-party HTTP dependency — stdlib only.
"""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable


@runtime_checkable
class CutoverStrategy(Protocol):
    def cutover(self, neo, alias: str, new_physical: str, old_physical: str | None) -> None: ...


class AliasSwapCutover:
    """Default — `ALTER ALIAS … SET DATABASE TARGET` (today's behaviour)."""

    def cutover(self, neo, alias: str, new_physical: str, old_physical: str | None) -> None:
        if old_physical is None:  # a first load (#107): the alias doesn't exist yet
            neo.create_alias(alias, new_physical)
        else:
            neo.alter_alias(alias, new_physical)


class ExternalRoutingCutover:
    """Invoke a configured hook so an external router repoints; the Neo4j alias is left
    untouched. Rollback is the router's concern (repoint back to `old_physical`)."""

    def __init__(self, hook: str):
        if not hook:
            raise RuntimeError("external cutover needs CUTOVER_HOOK (http(s) URL or command)")
        self.hook = hook

    def cutover(self, neo, alias: str, new_physical: str, old_physical: str | None) -> None:
        call_hook(self.hook, {"alias": alias, "new_physical": new_physical,
                              "old_physical": old_physical}, env_prefix="CUTOVER_")


def call_hook(hook: str, payload: dict, *, env_prefix: str, timeout: float = 60) -> None:
    """Call a hook: an http(s) URL is POSTed the JSON payload (non-2xx raises); anything else
    is a command run with each payload field as `<env_prefix><FIELD>` (non-zero exit raises).
    Shared by the cutover (#17) and the load notice (#126)."""
    if hook.startswith(("http://", "https://")):
        import json
        import urllib.request

        req = urllib.request.Request(hook, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # non-2xx raises HTTPError
            resp.read()
        return
    import json
    import shlex
    import subprocess

    env = {**os.environ, **{f"{env_prefix}{k.upper()}": v if isinstance(v, str) else
                            ("" if v is None else json.dumps(v)) for k, v in payload.items()}}
    subprocess.run(shlex.split(hook), check=True, env=env, timeout=timeout)


def from_env() -> CutoverStrategy:
    name = os.environ.get("CUTOVER_STRATEGY", "alias-swap")
    if name == "alias-swap":
        return AliasSwapCutover()
    if name == "external":
        return ExternalRoutingCutover(os.environ.get("CUTOVER_HOOK", ""))
    raise RuntimeError(f"unknown CUTOVER_STRATEGY {name!r}; known: alias-swap, external")


def for_cluster(cfg) -> CutoverStrategy:
    """A cluster's `cutover:` setting (#106), else the environment's."""
    if cfg is None:
        return from_env()
    if cfg.strategy == "external":
        return ExternalRoutingCutover(cfg.hook or "")
    return AliasSwapCutover()
