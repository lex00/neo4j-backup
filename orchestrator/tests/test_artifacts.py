"""#118: artifact sidecars from `neo4j-admin backup inspect`, chains read from them, and the ops
that keep them in step (backup, aggregate, prune, backfill)."""

import json
from datetime import datetime, timezone

from neo4j_backup_core import artifacts, ops, paths
from neo4j_backup_core.clients import BackupRunner

LAYOUT = paths.DefaultPathLayout()
RUNNER = BackupRunner(scratch_path="/scratch")
T = datetime(2026, 11, 15, tzinfo=timezone.utc)


def rec(file, full, lo, hi, time, db="orders-a", folder="demo/orders/orders-a/", dbid="id-1"):
    return {"uri": f"s3://bucket/{folder}{file}", "database": db, "databaseId": dbid,
            "time": time, "compressed": True, "full": full, "lowestTransaction": lo,
            "highestTransaction": hi, "storeIdHash": -1, "recovered": full}


def inspect_output(records):
    # what --verbose prints: log lines, the version line, then the JSON with no trailing newline
    return ("Executing command line: java ...\nneo4j 2026.05.0\nVM Name: OpenJDK\n"
            "2026-11-15 INFO [c.n.b.i.InspectBackupExecutor] Inspecting path: /x/\n"
            + json.dumps(records))


class MemStore:
    """An object store held in a dict: `.backup` objects are artifacts, the rest text."""

    def __init__(self):
        self.objects: dict[str, str] = {}

    def uri(self, key):
        return f"s3://bucket/{key}"

    def add(self, key):
        self.objects[key] = "<backup>"

    def list_artifacts(self, prefix):
        return [(k, 1, T) for k in sorted(self.objects)
                if k.startswith(prefix) and k.endswith(".backup")]

    def latest_artifact_key(self, prefix):
        arts = self.list_artifacts(prefix)
        return arts[-1][0] if arts else None

    def list_text_keys(self, prefix, suffix=".cypher"):
        return [(k, T) for k in sorted(self.objects) if k.startswith(prefix) and k.endswith(suffix)]

    def put_text(self, key, text):
        self.objects[key] = text

    def get_text(self, key):
        return self.objects[key]

    def delete_keys(self, keys):
        for k in keys:
            self.objects.pop(k, None)
        return len(keys)


FOLDER = "demo/orders/orders-a/"
RECS = [rec("f1.backup", True, 1, 13, "2026-11-15T21:26:56"),
        rec("d1.backup", False, 14, 14, "2026-11-15T21:27:25"),
        rec("d2.backup", False, 0, 0, "2026-11-15T21:27:37")]


def admin(records):
    calls = []

    def run(cmd):
        calls.append(cmd)
        return inspect_output(records) if "inspect" in cmd else "backup done\n"
    return calls, run


def test_parse_inspect_finds_records_and_version():
    version, recs = artifacts.parse_inspect(inspect_output(RECS))
    assert version == "2026.05.0"
    assert [r["type"] for r in recs] == ["full", "diff", "diff"]
    assert recs[1]["lowest_tx"] == 14 and recs[0]["file"] == "f1.backup"


def test_backup_writes_a_sidecar_per_artifact():
    store = MemStore()
    for r in RECS[:1]:
        store.add(FOLDER + r["uri"].rsplit("/", 1)[1])
    calls, run = admin(RECS[:1])
    ops.run_backup(run, RUNNER, store, "orders-a", FOLDER, "FULL")
    assert calls[1][:5] == ["neo4j-admin", "backup", "inspect", "--format=JSON", "--empty"]
    meta = json.loads(store.objects[FOLDER + "f1.backup.meta.json"])
    assert meta["type"] == "full" and meta["highest_tx"] == 13 and meta["neo4j_admin"] == "2026.05.0"


def test_later_backup_only_writes_missing_sidecars():
    store = MemStore()
    store.add(FOLDER + "f1.backup")
    ops.run_backup(admin(RECS[:1])[1], RUNNER, store, "orders-a", FOLDER, "FULL")
    store.objects[FOLDER + "f1.backup.meta.json"] = json.dumps({"marker": True, "type": "full"})
    store.add(FOLDER + "d1.backup")
    ops.run_backup(admin(RECS[:2])[1], RUNNER, store, "orders-a", FOLDER, "DIFF")
    assert json.loads(store.objects[FOLDER + "f1.backup.meta.json"]) == {"marker": True, "type": "full"}
    assert json.loads(store.objects[FOLDER + "d1.backup.meta.json"])["type"] == "diff"


def test_inspect_failure_does_not_fail_the_backup():
    store = MemStore()
    store.add(FOLDER + "f1.backup")
    logs = []

    def run(cmd):
        if "inspect" in cmd:
            raise RuntimeError("inspect broke")
        return ""
    key = ops.run_backup(run, RUNNER, store, "orders-a", FOLDER, "FULL", log=logs.append)
    assert key == FOLDER + "f1.backup" and "inspect broke" in logs[0]
    assert artifacts.read(store, FOLDER)[1] == [FOLDER + "f1.backup"]  # left unknown


def test_aggregate_rewrites_sidecars_and_drops_orphans():
    store = MemStore()
    for f in ("f1.backup", "d1.backup", "d2.backup"):
        store.add(FOLDER + f)
    ops.run_backup(admin(RECS)[1], RUNNER, store, "orders-a", FOLDER, "AUTO")
    # aggregate collapses the chain into one recovered full with a new name
    for f in ("f1.backup", "d1.backup", "d2.backup"):
        del store.objects[FOLDER + f]
    store.add(FOLDER + "r1.backup")
    recovered = [rec("r1.backup", True, 1, 14, "2026-11-15T21:30:00")]
    ops.run_aggregate(admin(recovered)[1], RUNNER, store, "orders-a", FOLDER)
    assert sorted(k for k in store.objects if k.endswith(".meta.json")) == \
        [FOLDER + "r1.backup.meta.json"]


def test_chains_group_by_folder_and_start_at_each_full():
    store = MemStore()
    for f in ("f1.backup", "d1.backup", "d2.backup", "f2.backup", "d3.backup"):
        store.add(FOLDER + f)
    store.add("demo/orders/orders-b/x.backup")  # no sidecar
    recs = RECS + [rec("f2.backup", True, 1, 20, "2026-11-16T02:00:00"),
                   rec("d3.backup", False, 21, 25, "2026-11-16T03:00:00")]
    ops.record_artifacts(admin(recs)[1], RUNNER, store, FOLDER)
    out = artifacts.chains(store, "demo/orders/")
    assert [len(c["artifacts"]) for c in out["chains"]] == [3, 2]
    first = out["chains"][0]
    assert (first["from"], first["to"], first["highest_tx"]) == \
        ("2026-11-15T21:26:56", "2026-11-15T21:27:37", 14)
    assert out["unknown"] == ["demo/orders/orders-b/x.backup"]


def test_chains_flag_diffs_without_a_full():
    store = MemStore()
    store.add(FOLDER + "d1.backup")
    ops.record_artifacts(admin([RECS[1]])[1], RUNNER, store, FOLDER)
    out = artifacts.chains(store, "demo/orders/")
    assert out["chains"][0]["broken"]


def test_artifact_chains_backfills_unknown_once():
    store = MemStore()
    store.add(FOLDER + "f1.backup")
    calls, run = admin(RECS[:1])
    out = ops.artifact_chains(store, LAYOUT, "demo", "orders", run_admin=run, runner=RUNNER)
    assert out["unknown"] == [] and len(out["chains"]) == 1
    ops.artifact_chains(store, LAYOUT, "demo", "orders", run_admin=run, runner=RUNNER)
    assert len(calls) == 1  # second read found the sidecar, no inspect


def test_prune_deletes_sidecars_with_their_artifacts():
    store = MemStore()
    for f in ("f1.backup", "d1.backup"):
        store.add(FOLDER + f)
    ops.record_artifacts(admin(RECS[:2])[1], RUNNER, store, FOLDER)
    del store.objects[FOLDER + "f1.backup"]  # as if prune deleted it
    assert artifacts.drop_orphans(store, "demo/orders/") == [FOLDER + "f1.backup.meta.json"]
    assert FOLDER + "d1.backup.meta.json" in store.objects


def test_chains_split_two_stores_in_one_folder():
    # #120: a by-name restore recreates the database under the same name; AUTO falls back to a
    # full in the same folder. Seen on the stack: same storeIdHash, different databaseId.
    store = MemStore()
    recs = [rec("f1.backup", True, 1, 5, "2026-11-15T21:57:42", dbid="A"),
            rec("d1.backup", False, 6, 6, "2026-11-15T21:57:53", dbid="A"),
            rec("f2.backup", True, 1, 7, "2026-11-15T21:58:08", dbid="B"),
            rec("d2.backup", False, 8, 8, "2026-11-15T21:58:27", dbid="B"),
            # a diff of A taken late (by time) must not join B's chain
            rec("d3.backup", False, 7, 7, "2026-11-15T21:59:00", dbid="A")]
    for r in recs:
        store.add(FOLDER + r["uri"].rsplit("/", 1)[1])
    ops.record_artifacts(admin(recs)[1], RUNNER, store, FOLDER)
    out = artifacts.chains(store, "demo/orders/")
    got = [(c["database_id"], [a["key"].rsplit("/", 1)[1] for a in c["artifacts"]])
           for c in out["chains"]]
    assert got == [("A", ["f1.backup", "d1.backup", "d3.backup"]),
                   ("B", ["f2.backup", "d2.backup"])]
