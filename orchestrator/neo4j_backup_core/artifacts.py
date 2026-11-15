"""What each backup artifact is: type, transaction range, time and store (#118).

neo4j-admin's artifact names don't say whether a file is a full or a diff, and an object's
LastModified is upload time, so the tool records each artifact's own metadata in a sidecar object
next to it, `<artifact>.meta.json`, written from `neo4j-admin backup inspect`. Sidecars are plain
objects so every cloud backend handles them the same way.

A sidecar holds the inspect record (database, databaseId, time, full, lowestTransaction,
highestTransaction, storeIdHash, recovered, compressed) under snake_case names, plus
`neo4j_admin` (the version that wrote it) and `key`. neo4j-admin reports one `time` per artifact,
the moment the backup was taken; there are no per-transaction times.
"""

from __future__ import annotations

import json
import posixpath
import re

SUFFIX = ".meta.json"

_FIELDS = {
    "database": "database", "databaseId": "database_id", "time": "time", "full": "full",
    "lowestTransaction": "lowest_tx", "highestTransaction": "highest_tx",
    "storeIdHash": "store_id_hash", "recovered": "recovered", "compressed": "compressed",
}
_VERSION = re.compile(r"^neo4j (\d[\w.\-]*)\s*$", re.M)


def sidecar_key(key: str) -> str:
    return key + SUFFIX


def parse_inspect(output: str) -> tuple[str | None, list[dict]]:
    """(neo4j-admin version, records) from `backup inspect --format=JSON --verbose` output. The
    JSON array is found wherever it sits among the log lines (it may have no trailing newline)."""
    m = _VERSION.search(output)
    version = m.group(1) if m else None
    dec = json.JSONDecoder()
    i = output.find("[")
    while i != -1:
        try:
            value, _end = dec.raw_decode(output, i)
        except ValueError:
            value = None
        if isinstance(value, list) and all(isinstance(r, dict) for r in value) and (
                not value or "uri" in value[0]):
            return version, [_normalise(r) for r in value]
        i = output.find("[", i + 1)
    raise ValueError("no JSON records in neo4j-admin backup inspect output")


def _normalise(rec: dict) -> dict:
    out = {new: rec.get(old) for old, new in _FIELDS.items()}
    out["file"] = posixpath.basename(rec.get("uri", ""))
    out["type"] = "full" if rec.get("full") else "diff"
    return out


def write_sidecars(store, prefix: str, output: str, *, only: set[str] | None = None) -> list[str]:
    """Write one sidecar per inspected artifact under `prefix` (matched by file name). `only`
    limits it to those file names. Returns the sidecar keys written."""
    version, records = parse_inspect(output)
    written = []
    for rec in records:
        if only is not None and rec["file"] not in only:
            continue
        key = prefix + rec["file"]
        body = {**rec, "key": key, "neo4j_admin": version}
        store.put_text(sidecar_key(key), json.dumps(body, sort_keys=True))
        written.append(sidecar_key(key))
    return written


def drop_orphans(store, prefix: str) -> list[str]:
    """Delete sidecars under `prefix` whose artifact no longer exists (aggregate, prune)."""
    live = {k for (k, _s, _m) in store.list_artifacts(prefix)}
    orphans = [k for (k, _m) in store.list_text_keys(prefix, SUFFIX)
               if k[: -len(SUFFIX)] not in live]
    if orphans:
        store.delete_keys(orphans)
    return orphans


def read(store, prefix: str) -> tuple[dict[str, dict], list[str]]:
    """({artifact key: sidecar}, [artifact keys with no sidecar]) under `prefix`."""
    metas: dict[str, dict] = {}
    for k, _m in store.list_text_keys(prefix, SUFFIX):
        metas[k[: -len(SUFFIX)]] = json.loads(store.get_text(k))
    keys = [k for (k, _s, _m) in store.list_artifacts(prefix)]
    return {k: metas[k] for k in keys if k in metas}, [k for k in keys if k not in metas]


def chains(store, prefix: str) -> dict:
    """The backup chains under `prefix` (an alias or physical prefix), grouped by the folder each
    artifact sits in (one physical). Each chain starts at a full and runs through the diffs after
    it, in transaction order; its window is the full's time to the newest artifact's time.
    Artifacts without a sidecar are listed under `unknown`."""
    metas, unknown = read(store, prefix)
    by_folder: dict[str, list[dict]] = {}
    for key, meta in metas.items():
        by_folder.setdefault(posixpath.dirname(key) + "/", []).append(meta)
    out = []
    for folder in sorted(by_folder):
        arts = sorted(by_folder[folder], key=lambda r: (r.get("time") or "", r["type"] != "full"))
        current: dict | None = None
        orphans: list[dict] = []
        for a in arts:
            entry = {k: a.get(k) for k in ("key", "type", "lowest_tx", "highest_tx", "time")}
            if a["type"] == "full":
                current = {"folder": folder, "database": a.get("database"),
                           "store_id_hash": a.get("store_id_hash"), "artifacts": [entry]}
                out.append(current)
            elif current is None:
                orphans.append(entry)  # diffs with no full before them in this folder
            else:
                current["artifacts"].append(entry)
        if orphans:
            out.append({"folder": folder, "database": arts[0].get("database"),
                        "store_id_hash": arts[0].get("store_id_hash"), "artifacts": orphans,
                        "broken": "no full backup before these diffs"})
    for c in out:
        c["from"] = c["artifacts"][0]["time"]
        c["to"] = c["artifacts"][-1]["time"]
        c["highest_tx"] = max((a["highest_tx"] or 0) for a in c["artifacts"])
    return {"prefix": prefix, "chains": out, "unknown": sorted(unknown)}
