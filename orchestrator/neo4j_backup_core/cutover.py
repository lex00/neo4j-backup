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
        neo.alter_alias(alias, new_physical)


class ExternalRoutingCutover:
    """Invoke a configured hook so an external router repoints; the Neo4j alias is left
    untouched. Rollback is the router's concern (repoint back to `old_physical`)."""

    def __init__(self, hook: str):
        if not hook:
            raise RuntimeError("external cutover needs CUTOVER_HOOK (http(s) URL or command)")
        self.hook = hook

    def cutover(self, neo, alias: str, new_physical: str, old_physical: str | None) -> None:
        payload = {"alias": alias, "new_physical": new_physical, "old_physical": old_physical}
        if self.hook.startswith(("http://", "https://")):
            import json
            import urllib.request

            req = urllib.request.Request(
                self.hook, data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req) as resp:  # non-2xx raises HTTPError
                resp.read()
        else:
            import shlex
            import subprocess

            env = {
                **os.environ,
                "CUTOVER_ALIAS": alias,
                "CUTOVER_NEW_PHYSICAL": new_physical,
                "CUTOVER_OLD_PHYSICAL": old_physical or "",
            }
            subprocess.run(shlex.split(self.hook), check=True, env=env)


def from_env() -> CutoverStrategy:
    name = os.environ.get("CUTOVER_STRATEGY", "alias-swap")
    if name == "alias-swap":
        return AliasSwapCutover()
    if name == "external":
        return ExternalRoutingCutover(os.environ.get("CUTOVER_HOOK", ""))
    raise RuntimeError(f"unknown CUTOVER_STRATEGY {name!r}; known: alias-swap, external")
