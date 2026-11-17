"""MCP prompts, one per operator workflow in `workflows/*.md` (#272). No `mcp` dependency: each
function returns the instructions an agent follows, and `server.py` registers them.

Every prompt takes a keyword-only `writes`: whether the server exposes the `run_*` tools. With
`writes=False` a step that would change something hands the operator the CLI command instead.
Every change is previewed first and waits for the operator's yes before `confirm=true`.
"""

from __future__ import annotations

_RULES = (
    "Rules: show the operator what each preview returns in a few plain lines. "
    "Never call a tool with confirm=true until the operator has said yes to that preview. "
    "If a tool returns ok=false, stop and show the error."
)


def _change(writes: bool, preview: str, call: str, cli: str, what: str, ticket: str = "") -> str:
    """One gated step: preview, ask, then apply. Read-only, a `run_*` preview isn't there, so the
    operator gets the CLI's --dry-run and --confirm commands instead."""
    t = f" --ticket {ticket}" if ticket else ""
    if writes:
        return (f"Call `{preview}` and show the operator {what}. Ask the operator to confirm. "
                f"Only after a yes, call `{call}`.")
    if not preview.startswith("run_"):
        return (f"Call `{preview}` and show the operator {what}. This server is read-only, so "
                f"after they review it give the operator the command: "
                f"`neo4j-backup {cli} --confirm{t}`.")
    return (f"This server is read-only. Give the operator `neo4j-backup {cli} --dry-run` to "
            f"review {what}, then `neo4j-backup {cli} --confirm{t}` to apply it.")


def _add(writes: bool, call: str, cli: str) -> str:
    """A step that only adds artifacts (backup, verify, metadata export): no confirm needed."""
    if writes:
        return f"Call `{call}`."
    return f"This server is read-only, so give the operator the command: `neo4j-backup {cli}`."


def _steps(title: str, steps: list[str]) -> str:
    body = "\n".join(f"{i}. {s}" for i, s in enumerate(steps, 1))
    return f"{title}\n\n{body}\n\n{_RULES}"


def onboard(group: str, policy_file: str = "", *, writes: bool = False) -> str:
    """Onboard a customer group: check the policy, put databases behind aliases, take and verify
    the first backup."""
    check = (f"Call `validate_policy(file='{policy_file}')`. If it is not valid, show the errors "
             f"and stop." if policy_file else
             "Ask the operator for the policy file path and call `validate_policy` on it. If it "
             "is not valid, show the errors and stop.")
    return _steps(f"Onboard group `{group}` (workflows/onboard.md).", [
        check,
        f"Call `policy_show(group='{group}')` and tell the operator the tier, retention_days and "
        f"rpo_minutes the group gets.",
        _change(writes, f"preview_adopt(group='{group}')",
                f"run_adopt(group='{group}', confirm=true)", f"adopt {group}",
                "each member's action. If every member is `skip`, the aliases exist; go on. "
                "A `conflict` means an alias points elsewhere: stop. A `migrate` is an outage; "
                "stop and point the operator to workflows/onboard.md"),
        _add(writes, f"run_backup(group='{group}', kind='FULL')", f"backup {group} --kind FULL"),
        _add(writes, f"run_verify(group='{group}')", f"verify {group}"),
        f"Call `coverage_report(group='{group}')` and confirm every member is `ok`.",
        "Remind the operator to tell the customer their tier and retention_days and to send "
        "them CUSTOMERS.md.",
    ])


def check_coverage(tenant: str = "", *, writes: bool = False) -> str:
    """Check that every target has a fresh backup, and fix what is stale."""
    scope = f"tenant='{tenant}'" if tenant else ""
    stale = ("For each stale member, call the `next` tool call the report gives, one group at a "
             "time, then call `coverage_report` again." if writes else
             "For each stale member, give the operator the CLI form of its `next` step "
             "(`neo4j-backup backup <group>`).")
    return _steps("Check backup coverage (workflows/coverage.md).", [
        f"Call `coverage_report({scope})`. Give the operator the summary counts.",
        "List each stale member with its `why`. If there are none, say all targets are fresh "
        "and stop.",
        stale,
        "List `retired` groups with their deletes_on day and any `unmanaged` folders, and say "
        "the operator decides what to do with those.",
    ])


def restore_request(group: str, until: str = "", ticket: str = "", *,
                    writes: bool = False) -> str:
    """Restore a group to a point in time on a customer's request."""
    u = f", until='{until}'" if until else ""
    t = f", ticket='{ticket}'" if ticket else ""
    ask = [] if ticket else ["Ask the operator for a ticket id and use it below."]
    reason = ", reason='<the customer's request in a few words>'"
    return _steps(f"Restore group `{group}` on request (workflows/restore.md).", [
        *ask,
        f"Call `restore_plan(group='{group}'{u}{t})`. If `covered` is false, show the operator "
        f"the error and `windows`, ask for another time, and call it again.",
        "Show the operator the instant each member goes back to, the database each alias "
        "serves now, any `warnings`, and the `message` for the customer.",
        _change(writes, f"preview_restore(group='{group}'{u})",
                f"run_restore(group='{group}'{u}, confirm=true{t}{reason})",
                f"restore {group}{' --until ' + until if until else ''}",
                "that the plan matches what restore_plan showed", ticket),
        "Check `baseline` in the result. If it failed, the restore still stands: say so and "
        "suggest " + (f"`run_backup(group='{group}', kind='FULL')`." if writes else
                      f"`neo4j-backup backup {group} --kind FULL`."),
        "Give the operator `old` and `newdb` per member for the customer. To undo: "
        + (f"`run_rollback(group='{group}')`, dry_run first." if writes else
           f"`neo4j-backup rollback {group} --dry-run`, then `--confirm`."),
    ])


def bulk_load(group: str, ticket: str = "", *, writes: bool = False) -> str:
    """Load or rebuild a group from its policy build."""
    t = f", ticket='{ticket}'" if ticket else ""
    return _steps(f"Load group `{group}` in bulk (workflows/load.md).", [
        f"Call `policy_show(group='{group}')` and show the operator the `load` settings: mode, "
        f"writes and build.",
        _change(writes, f"run_load(group='{group}', dry_run=true)",
                f"run_load(group='{group}', confirm=true{t})", f"load {group}",
                "the source objects, the build, the target databases and what is lost", ticket),
        "If the result has `failed_step`, show it: the new databases were dropped and the "
        "previous data still serves.",
        f"Call `coverage_report(group='{group}')` to show the baseline backup.",
    ])


def offboard(group: str, *, writes: bool = False) -> str:
    """Offboard a retired group: when its backups go, and what the operator does by hand."""
    return _steps(f"Offboard group `{group}` (workflows/change.md).", [
        f"Call `policy_show(group='{group}')`. If `state` is not `retired`, tell the operator to "
        f"set `state: retired` and `retired_on` in the policy and ship it first, and stop.",
        f"Call `coverage_report(group='{group}')` and give the deletes_on day.",
        _change(writes, f"run_export(group='{group}', dry_run=true)",
                f"run_export(group='{group}', confirm=true)", f"export {group}",
                "what a final portable dump per member would be made from, if the customer "
                "wants their data"),
        "Call `preview_prune` and show which of the group's artifacts retention deletes now.",
        "Tell the operator the steps the tool leaves to them: drop the aliases and databases "
        "after the last restore they might need, and remove the group from the policy once "
        "its data is gone.",
    ])


def recover_cluster(from_cluster: str, to_cluster: str, ticket: str = "", *,
                    writes: bool = False) -> str:
    """Restore every group of a lost cluster into another cluster."""
    t = f", ticket='{ticket}'" if ticket else ""
    args = f"from_cluster='{from_cluster}', to_cluster='{to_cluster}'"
    return _steps(f"Recover cluster `{from_cluster}` into `{to_cluster}` (workflows/recover.md).", [
        f"Call `list_targets()` and name the groups whose cluster is `{from_cluster}`.",
        f"Call `coverage_report()` and point out any of those groups that are stale: their "
        f"restore loses more.",
        _change(writes, f"run_restore_to_cluster({args}, dry_run=true)",
                f"run_restore_to_cluster({args}, confirm=true{t})",
                f"restore --from-cluster {from_cluster} --to-cluster {to_cluster}",
                "every group's seeds and the metadata it replays. If the source still answers, "
                "stop: a planned move needs fence_source=true, and that is the operator's call",
                ticket),
        "Show `baseline` per group. Tell the operator the policy is not edited: for a "
        f"permanent move they set `cluster: {to_cluster}` on the groups.",
    ])


PROMPTS = [onboard, check_coverage, restore_request, bulk_load, offboard, recover_cluster]
