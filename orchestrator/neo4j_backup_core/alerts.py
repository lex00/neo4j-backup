"""Failure alerts (#246): a JSON POST to NEO4J_BACKUP_ALERT_WEBHOOK when a run fails.

The body is `{"text": "..."}`, which Slack, Mattermost, Microsoft Teams (incoming webhooks) and
most chat tools accept, plus the details as fields. Nothing is sent when the variable is unset.
Used by the Dagster run-failure sensor and the Airflow failure callback; the CLI's equivalent
is a non-zero exit (`neo4j-backup status --stale` from cron).
"""

from __future__ import annotations

import json
import os
import urllib.request


def webhook() -> str | None:
    return os.environ.get("NEO4J_BACKUP_ALERT_WEBHOOK") or None


def message(where: str, name: str, error: str, link: str | None = None) -> dict:
    """The alert body: one line of text for chat, and the parts separately."""
    error = (error or "").strip().splitlines()[0][:500] if error else "failed"
    text = f"neo4j-backup: {where} {name} failed: {error}"
    if link:
        text += f" ({link})"
    return {"text": text, "source": where, "name": name, "error": error, "link": link}


def send(body: dict, url: str | None = None, timeout: float = 10) -> bool:
    """POST the body to the webhook; False (never an exception) when unset or it fails, so an
    alert problem doesn't hide the failure being reported."""
    url = url or webhook()
    if not url:
        return False
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return 200 <= r.status < 300
    except Exception:  # noqa: BLE001
        return False
