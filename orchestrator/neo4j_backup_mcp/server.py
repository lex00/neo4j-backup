"""FastMCP wiring for the operator MCP server (#58 P5). Import requires the optional `[mcp]` extra;
the tool logic lives in `tools.py` (no `mcp` dependency) so it can be tested without the SDK.

Scope + audit are the two safety layers on top of the per-tool `confirm` guard:

- **Scope.** `NEO4J_BACKUP_MCP_MODE` = `read-only` (default) exposes only the status/preview tools.
  `read-write` additionally exposes the mutating tools — but each of those still refuses without
  `confirm=true`. So enabling mutations is a deliberate server-config decision, separate from any
  single call.
- **Audit.** Every tool invocation is logged (name + arguments + outcome) to stderr. Arguments here
  are targets/flags, never secrets — credentials come from the environment, not tool inputs.

Transport is stdio: the operator's agent spawns the server as a local subprocess, so the process
boundary is the auth boundary. Do not expose it over an unauthenticated network transport with
`read-write` set.
"""

from __future__ import annotations

import functools
import logging
import os
import sys

from neo4j_backup_core import ops
from neo4j_backup_mcp import tools

log = logging.getLogger("neo4j_backup_mcp")


def _import_fastmcp():
    """Import FastMCP, or exit with a clear pointer to the extra (the tool logic in `tools.py`
    needs no SDK; only running the server does)."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        sys.exit("neo4j-backup-mcp needs the MCP SDK — install the extra: "
                 "pip install 'neo4j-backup-dagster[mcp] @ git+https://github.com/lex00/neo4j-backup"
                 "@v0.4.0#subdirectory=orchestrator'")
    return FastMCP


def _audited(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        log.info("tool %s args=%s", fn.__name__, kwargs or dict(enumerate(args)))
        try:
            result = fn(*args, **kwargs)
        except ops.OpError as e:
            log.warning("tool %s refused: %s", fn.__name__, e)
            return {"ok": False, "error": {"kind": "op_failed", "msg": str(e)}}
        log.info("tool %s ok", fn.__name__)
        return result
    return wrapper


def build_server(mode: str | None = None) -> FastMCP:
    """Construct the server with the read/write scope from `mode` (or NEO4J_BACKUP_MCP_MODE)."""
    mode = mode or os.environ.get("NEO4J_BACKUP_MCP_MODE", "read-only")
    mcp = _import_fastmcp()("neo4j-backup")
    for fn in tools.READ_TOOLS:
        mcp.tool()(_audited(fn))
    if mode == "read-write":
        for fn in tools.WRITE_TOOLS:
            mcp.tool()(_audited(fn))
        log.info("neo4j-backup MCP: read-write scope (mutations exposed; each requires confirm=true)")
    else:
        log.info("neo4j-backup MCP: read-only scope (set NEO4J_BACKUP_MCP_MODE=read-write to mutate)")
    return mcp


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    build_server().run()  # stdio transport


if __name__ == "__main__":
    main()
