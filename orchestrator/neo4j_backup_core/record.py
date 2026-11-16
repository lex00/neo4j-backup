"""Why a mutating operation ran (#119): a ticket reference and a reason.

Approvals stay with the platform team's own process; the tool only records what it was told.
Every mutating entry point (CLI guarded commands, MCP write tools, the Dagster restore job, the
Airflow restore DAG) takes `ticket` and `reason`, logs them, and returns them with the result.
`NEO4J_BACKUP_REQUIRE_TICKET=true` makes a ticket mandatory for anything that applies a change
(a dry run never needs one).
"""

from __future__ import annotations

import os

MAX_LEN = 500


class TicketError(ValueError):
    kind = "ticket_required"


def require_ticket() -> bool:
    return os.environ.get("NEO4J_BACKUP_REQUIRE_TICKET", "").lower() in ("1", "true", "yes")


def fields(ticket: str | None, reason: str | None, *, applying: bool) -> dict:
    """{"ticket": …, "reason": …} after checking them. Raises TicketError when a ticket is
    required for an applied change and missing, or a value is too long or multi-line."""
    out = {}
    for name, value in (("ticket", ticket), ("reason", reason)):
        value = (value or "").strip() or None
        if value is not None and (len(value) > MAX_LEN or "\n" in value or "\r" in value):
            raise TicketError(f"{name} must be one line of at most {MAX_LEN} characters")
        out[name] = value
    if applying and require_ticket() and not out["ticket"]:
        raise TicketError("a ticket is required for changes here (NEO4J_BACKUP_REQUIRE_TICKET); "
                          "pass --ticket / ticket=")
    return out


def describe(op: str, rec: dict) -> str:
    """One log line for the run logs."""
    return (f"{op}: ticket={rec.get('ticket') or '-'} "
            f"reason={rec.get('reason') or '-'}")
