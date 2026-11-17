"""End-to-end smoke of the operator MCP server (#58 P5) against the local compose stack.

Spawns `neo4j_backup_mcp` over stdio with a real MCP client, in read-only scope, and calls the
read tools (list_targets, backup_status, coverage_report, restore_plan, validate_policy,
preview_adopt, preview_prune) and the workflow prompts (#272, #274) against the running MinIO + policy — a full
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

            prompts = {p.name for p in (await session.list_prompts()).prompts}
            want = {"onboard", "check_coverage", "restore_request", "bulk_load", "offboard",
                    "recover_cluster"}
            assert prompts == want, prompts
            got = await session.get_prompt("restore_request", {"group": "demo"})
            text = got.messages[0].content.text
            assert "restore_plan(group='demo')" in text and "`run_" not in text, text
            print(f"== prompts: {sorted(prompts)}; restore_request is read-only form ==")

            cov = _payload(await session.call_tool("coverage_report", {}))
            assert "summary" in cov and cov["summary"]["ok"] + cov["summary"]["stale"] > 0, cov
            print(f"== coverage_report: {cov['summary']} ==")

            plan = _payload(await session.call_tool("restore_plan", {"group": "demo",
                                                                     "ticket": "SMOKE-1"}))
            assert plan["covered"] is True and "SMOKE-1" in plan["message"], plan
            print(f"== restore_plan: {plan['message']} ==")

            old = _payload(await session.call_tool("restore_plan", {
                "group": "demo", "until": "2000-01-01T00:00:00Z"}))
            assert old["covered"] is False and old["windows"], old
            print(f"== restore_plan before any backup: covered=false, {old['error'][:80]} ==")

            valid = _payload(await session.call_tool("validate_policy",
                                                     {"file": "policies/demo.yaml"}))
            assert valid["valid"] is True, valid
            print("== validate_policy policies/demo.yaml: valid ==")

            adopt = _payload(await session.call_tool("preview_adopt", {"group": "demo"}))
            assert adopt["plan"]["members"], adopt
            print(f"== preview_adopt: {[m['action'] for m in adopt['plan']['members']]} ==")

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
