"""A per-group lock, `_locks/<group>.lock` in the default store, created with the store's
conditional write (atomic on S3, GCS and Azure). Every front-end takes it around a member's
backup, verify, aggregate and the group's load: the CLI, the Airflow tasks and the Dagster ops,
so their work on one group never overlaps whatever started it. It expires after `ttl` seconds
(6 hours), so a crashed run doesn't hold it forever; a waiting run gives up after
NEO4J_BACKUP_LOCK_WAIT seconds. To clear a lock left by a crash sooner, delete the object.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager


class Locked(RuntimeError):
    """The group's lock is held; `holder` says by what."""

    def __init__(self, group: str, holder: dict):
        super().__init__(f"group {group!r} is busy: {holder.get('what')} since "
                         f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(holder.get('at', 0)))}")
        self.holder = holder


def _key(group: str) -> str:
    return f"_locks/{group}.lock"


def acquire(store, group: str, what: str, ttl: int = 6 * 3600, wait: float = 0,
            poll: float = 10, sleep=time.sleep) -> str:
    """Take the lock, waiting up to `wait` seconds for it, or raise Locked. Returns a token."""
    deadline = time.time() + wait
    while True:
        try:
            return _try(store, group, what, ttl)
        except Locked:
            if time.time() >= deadline:
                raise
            sleep(poll)


def _try(store, group: str, what: str, ttl: int) -> str:
    token = f"{os.uname().nodename}-{os.getpid()}-{time.time_ns()}"
    body = json.dumps({"what": what, "token": token, "at": time.time(),
                       "expires": time.time() + ttl})
    for _ in range(2):
        if store.create_exclusive(_key(group), body):
            return token
        try:
            held = json.loads(store.get_text(_key(group)))
        except Exception:  # noqa: BLE001 - vanished between the two calls: try again
            continue
        if held.get("expires", 0) > time.time():
            raise Locked(group, held)
        store.delete_keys([_key(group)])  # expired: clear it and try once more
    raise Locked(group, {"what": "another task (lost the race)", "at": time.time()})


def release(store, group: str, token: str) -> None:
    try:
        held = json.loads(store.get_text(_key(group)))
    except Exception:  # noqa: BLE001 - already gone
        return
    if held.get("token") == token:
        store.delete_keys([_key(group)])


@contextmanager
def group_lock(store, group: str, what: str, ttl: int = 6 * 3600, wait: float = 0):
    token = acquire(store, group, what, ttl, wait=wait)
    try:
        yield token
    finally:
        release(store, group, token)
