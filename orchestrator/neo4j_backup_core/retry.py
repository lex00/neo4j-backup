"""Transient-failure classification for Bolt operations.

The rule (see #25): never branch on exception *message* text — it is version-dependent and
localizable. Classify retryability by exception **type** and Neo4j status **code**
(`err.code`, a GQLSTATUS-style string like ``Neo.ClientError.Cluster.NotALeader``). The
leader-re-election and auth-expiry cases are `ClientError`-family codes whose Python class is
not blanket-retryable (and in newer drivers isn't even a `ClientError` subclass), so they are
matched by code, not by `isinstance`.

`retry_bolt` (added in #19) consumes these predicates.
"""

from __future__ import annotations

import os
import time
from typing import Callable, TypeVar

from neo4j.exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

T = TypeVar("T")

# Whole exception classes that are always transient.
RETRYABLE_EXC = (ServiceUnavailable, SessionExpired, TransientError)

# Auth/token expiry: retryable, but the driver must be rebuilt — the cached token is dead, so
# retrying on the same driver just fails again.
AUTH_EXPIRED_CODES = frozenset({
    "Neo.ClientError.Security.TokenExpired",
    "Neo.ClientError.Security.AuthorizationExpired",
})

# Specific status codes that are retryable despite their class not being blanket-retryable:
# leader re-election / a write routed to a read-only member, plus auth-expiry.
RETRYABLE_CODES = frozenset({
    "Neo.ClientError.Cluster.NotALeader",
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
}) | AUTH_EXPIRED_CODES


def error_code(exc: BaseException) -> str | None:
    """The Neo4j status code for a driver error, or None for non-Neo4j errors. Typed access —
    never parse messages."""
    return exc.code if isinstance(exc, Neo4jError) else None


def is_retryable(exc: BaseException) -> bool:
    """True if `exc` is a known-transient Bolt failure (by class or status code)."""
    if isinstance(exc, RETRYABLE_EXC):
        return True
    return error_code(exc) in RETRYABLE_CODES


def is_auth_expired(exc: BaseException) -> bool:
    """True if `exc` is an expired-token/authorization error — retryable only after the driver
    is rebuilt with a freshly resolved credential (see #18)."""
    return error_code(exc) in AUTH_EXPIRED_CODES


def retry_bolt(
    fn: Callable[[], T],
    *,
    attempts: int | None = None,
    base: float | None = None,
    cap: float | None = None,
    on_auth_expired: Callable[[], None] | None = None,
) -> T:
    """Call `fn`, retrying on transient Bolt failures with bounded exponential backoff.

    Retries only `is_retryable` errors; anything else (bad Cypher, real auth failure) is
    re-raised immediately. On an auth-expired error, `on_auth_expired()` is invoked before the
    backoff so a cached driver can be dropped and its credential re-resolved (#18).

    Defaults come from the environment (overridable per call, mainly for tests):
    `NEO4J_RETRY_ATTEMPTS` (5), `NEO4J_RETRY_BASE` seconds (0.2), `NEO4J_RETRY_CAP` seconds (5).
    """
    attempts = attempts if attempts is not None else int(os.environ.get("NEO4J_RETRY_ATTEMPTS", "5"))
    base = base if base is not None else float(os.environ.get("NEO4J_RETRY_BASE", "0.2"))
    cap = cap if cap is not None else float(os.environ.get("NEO4J_RETRY_CAP", "5.0"))
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — re-raised below unless known-transient
            if not is_retryable(exc) or attempt >= attempts:
                raise
            if is_auth_expired(exc) and on_auth_expired is not None:
                on_auth_expired()
            time.sleep(min(cap, base * (2 ** (attempt - 1))))
