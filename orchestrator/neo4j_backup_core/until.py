"""Point-in-time values for `seedRestoreUntil` (#77).

`--until` (CLI), `until` (MCP, where an agent supplies it) and `restore_until` (Dagster config,
Airflow param) are parsed here at the boundary, and again just before the value is written into
Cypher, so only a normalised datetime string ever reaches the statement. A value without a
timezone is UTC, as the docs' examples assume.
"""

from __future__ import annotations

from datetime import datetime, timezone


class UntilError(ValueError):
    """Not an ISO-8601 date-time."""


def parse_until(value: str | None) -> str | None:
    """`value` as a normalised UTC ISO-8601 string (`2026-07-11T14:32:00+00:00`), or None for
    None/empty. Raises UntilError for anything that isn't an ISO-8601 date-time."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise UntilError(f"until must be an ISO-8601 string, got {type(value).__name__}")
    text = value.strip()
    if len(text) > 40 or "T" not in text.upper():
        raise UntilError(f"until {value!r} is not an ISO-8601 date-time (e.g. 2026-07-11T14:32:00Z)")
    if text[-1] in "zZ":
        text = text[:-1] + "+00:00"  # Python 3.10's fromisoformat doesn't take Z
    try:
        t = datetime.fromisoformat(text)
    except ValueError as e:
        raise UntilError(f"until {value!r} is not an ISO-8601 date-time "
                         f"(e.g. 2026-07-11T14:32:00Z): {e}") from None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).isoformat()
