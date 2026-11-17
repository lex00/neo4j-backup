"""The agent's side of `just try-mcp` (#280): the `restore_request` prompt played over a real MCP
session with the server in read-write scope. A real agent decides each call from the prompt's
steps; this script makes the same calls in order, and says yes where the operator would.

Usage: python3 bootstrap/try_mcp.py <group> <until> <ticket>   (env as for the CLI)
"""
import asyncio
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

B, N = "\033[1m", "\033[0m"


def payload(result):
    if getattr(result, "structuredContent", None):
        doc = result.structuredContent
        return doc.get("result", doc) if set(doc) == {"result"} else doc
    return json.loads(result.content[0].text)


def short(ts):
    return str(ts or "").split("T")[-1][:8]


def say(line=""):
    print(f"   {line}")


async def call(session, tool, args):
    shown = ", ".join(f"{k}={v!r}" for k, v in args.items())
    print(f"{B}agent -> {tool}({shown}){N}")
    out = payload(await session.call_tool(tool, args))
    if out.get("ok") is False and not out.get("needs_confirmation"):
        print(json.dumps(out, indent=2))
        sys.exit(1)
    return out


async def main(group, until, ticket):
    log = open(".try.log", "a")
    env = {**os.environ, "NEO4J_BACKUP_MCP_MODE": "read-write"}
    params = StdioServerParameters(command=sys.executable, args=["-m", "neo4j_backup_mcp"],
                                   env=env)
    async with stdio_client(params, errlog=log) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            prompt = await session.get_prompt("restore_request", {
                "group": group, "until": until, "ticket": ticket})
            steps = [ln for ln in prompt.messages[0].content.text.splitlines() if ln[:2] in
                     {f"{i}." for i in range(1, 10)}]
            print(f"{B}operator:{N} \"put {group} back to just before the delete, {ticket}\"")
            say(f"the agent loads the restore_request prompt: {len(steps)} steps, "
                f"a stop for your yes before the restore")

            plan = await call(session, "restore_plan",
                              {"group": group, "until": until, "ticket": ticket})
            for m in plan["points"]["members"]:
                for c in m["chains"]:
                    say(f"{m['member']:12} can go back to any time from {short(c['from'])} "
                        f"to {short(c['to'])}")
            say(f"message for the customer: {plan['message']}")

            prev = await call(session, "preview_restore", {"group": group, "until": until})
            members = prev["plan"]["members"]
            for m in members:
                what = (f"replay to {short(m['seed']['until'])}" if m["seed"].get("until")
                        else "no change after that time, seed its last backup")
                say(f"{m['alias']:12} {what}; then move the alias off {m['old']}")
            print(f"{B}agent:{N} \"This restores {len(members)} databases to {short(until)}. "
                  f"Go ahead?\"")
            print(f"{B}operator:{N} \"yes\"")

            done = await call(session, "run_restore", {
                "group": group, "until": until, "confirm": True, "ticket": ticket,
                "reason": "orders deleted by mistake"})
            for m in done["restored"]["members"]:
                say(f"{m['alias']:12} alias moved: {m['old']} -> {m['newdb']}")
            base = done.get("baseline") or []
            say(f"baseline full backups: {len(base)}")

            print(f"{B}agent:{N} \"Done. If they want it undone, rollback points back:\"")
            rb = await call(session, "run_rollback", {"group": group, "dry_run": True})
            for m in rb["plan"]["members"] if "plan" in rb else rb.get("members", []):
                say(f"{m['alias']:12} undo would point back at {m['newdb']}")


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:4]))
