"""End-to-end smoke of the operator MCP server (#58 P5) against the local compose stack.

Spawns `neo4j_backup_mcp` over stdio with a real MCP client, in read-only scope, and calls the
read tools (list_targets, backup_status, preview_prune) against the running MinIO + policy — a full
client↔server round trip. Also asserts the mutating tools are hidden in read-only scope.

Prereqs: the [mcp] extra installed and the stack up with a backup (`just fresh && just backup demo`).

    just mcp-smoke        # or: orchestrator/.venv/bin/python orchestrator/smoke_mcp.py
"""

import asyncio
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(REPO, "orchestrator", ".venv", "bin", "python")

ENV = {
    **os.environ,
    "NEO4J_BACKUP_MCP_MODE": "read-only",
    "NEO4J_BOLT_URI": os.environ.get("NEO4J_BOLT_URI", "neo4j://localhost:7687"),
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
    "NEO4J_BACKUP_POLICY": "policies/demo.yaml",
}


def _payload(result):
    if getattr(result, "structuredContent", None):
        return result.structuredContent
    return json.loads(result.content[0].text)


async def main() -> None:
    params = StdioServerParameters(command=PY, args=["-m", "neo4j_backup_mcp"], cwd=REPO, env=ENV)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            names = {t.name for t in (await session.list_tools()).tools}
            assert not any(n.startswith("run_") for n in names), f"mutations exposed read-only: {names}"
            print(f"== read-only scope: {len(names)} tools, no mutations ==")

            targets = _payload(await session.call_tool("list_targets", {}))
            assert targets["targets"], targets
            print(f"== list_targets: {targets['targets']} ==")

            status = _payload(await session.call_tool("backup_status", {}))
            assert status["targets"], status
            print(f"== backup_status: {len(status['targets'])} targets, "
                  f"e.g. {status['targets'][0]} ==")

            prune = _payload(await session.call_tool("preview_prune", {}))
            assert "keys" in prune and "deleted" in prune, prune
            print(f"== preview_prune: would delete {prune['deleted']} (inert) ==")

    print("PASS: neo4j-backup MCP server read-only round trip against the stack")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
