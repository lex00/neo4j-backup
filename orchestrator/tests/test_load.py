"""#107: load / rebuild a group (core op, with fakes)."""

import pytest

from neo4j_backup_core import load as L
from neo4j_backup_core import ops, paths
from neo4j_backup_core.clients import BackupRunner
from neo4j_backup_core.policy import Policy
from test_artifacts import MemStore

LAYOUT = paths.DefaultPathLayout()
TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"},
         "nightly": {"rebuild_cron": "0 1 * * *"}}
IMPORT_OUT = ("Imported Stats[processed=3, created=3, updated=0, deleted=0] nodes in 1ms\n"
              "Imported Stats[processed=1, created=1, updated=0, deleted=0] relationships in 1ms\n")


def group(writes="read-only", checks=(), names=("catalog",), mode="rebuild", restore="alias-swap"):
    key = "databases" if restore == "by-name" else "aliases"
    load = {"mode": mode, "writes": writes,
            "build": {"kind": "import", "input_type": "csv",
                      "args": ["--nodes=Product=products.csv", "--relationships=IN=in.csv"]},
            "checks": list(checks)}
    if writes == "read-write":
        load["protect_writes"] = False
    return Policy.model_validate({"db_groups": [{"id": "cat", key: list(names), "tier": "nightly",
                                                 "restore_mode": restore, "load": load}],
                                  "tiers": TIERS}).group("cat")


class Neo:
    def __init__(self, nodes=3, rels=1):
        self.nodes, self.rels = nodes, rels
        self.seeded, self.dropped, self.altered, self.ro, self.ran = [], [], [], [], []

    def alias_target(self, a):
        return f"{a}-old"

    def database_exists(self, n):
        return False

    def seed_database(self, n, uri, **k):
        self.seeded.append((n, uri))

    def set_read_only(self, n, ro=True):
        self.ro.append(n)

    def database_access(self, n):
        return "read-only" if n in self.ro else "read-write"

    def run_system(self, q, **p):
        self.ran.append(q)
        return [{"currentStatus": "online"}]

    def run_on(self, db, q):
        return [{"n": self.rels if "()-[r]->()" in q else self.nodes}]

    def drop_database(self, n):
        self.dropped.append(n)

    def privilege_commands(self, d):
        return []

    def alter_alias(self, a, t):
        self.altered.append((a, t))


def run_admin():
    calls = []
    return calls, lambda cmd, env=None: calls.append(cmd) or IMPORT_OUT


def test_resolve_args():
    assert L.resolve_args(["--nodes=P=a.csv,/abs.csv", "--relationships=s3://x/r.csv", "--x=y"],
                          lambda p: "s3://b/src/" + p) == \
        ["--nodes=P=s3://b/src/a.csv,/abs.csv", "--relationships=s3://x/r.csv", "--x=y"]


def test_source_prefix_per_member():
    assert L.source_prefix(group(), "catalog") == "cat/_source/"
    from neo4j_backup_core import naming  # the member's storage slug, as in its backup folder
    assert L.source_prefix(group(names=("a", "b")), "a") == f"cat/_source/{naming.slug('a')}/"


def test_plan_shows_source_build_target_and_loss():
    p = L.plan_load(Neo(), MemStore(), LAYOUT, group(writes="read-write"))
    m = p["members"][0]
    assert m["source"] == "s3://bucket/cat/_source/" and m["old"] == "catalog-old"
    assert "--nodes=Product=s3://bucket/cat/_source/products.csv" in m["build"]
    assert m["loses"] == "writes since the previous load"


def test_live_groups_have_nothing_to_load():
    g = Policy.model_validate({"db_groups": [{"id": "x", "aliases": ["a"], "tier": "gold"}],
                               "tiers": TIERS}).group("x")
    with pytest.raises(ops.OpError, match="live"):
        L.plan_load(Neo(), MemStore(), LAYOUT, g)


def test_load_builds_seeds_checks_sets_read_only_and_swaps():
    neo, store = Neo(), MemStore()
    calls, run = run_admin()
    out = L.load_group(run, BackupRunner(), neo, store, LAYOUT, group())
    m = out["members"][0]
    script = calls[0][2]
    assert "database import full load-catalog-" in script and "database dump" in script
    assert "--nodes=Product=s3://bucket/cat/_source/products.csv" in script
    assert m["dump"].startswith("cat/catalog/_seed/") and m["import_counts"] == \
        {"nodes": 3, "relationships": 1}
    assert neo.seeded[0][1] == "s3://bucket/" + m["dump"]
    assert neo.ro == [m["newdb"]]                       # read-only before the swap (#112)
    assert [c["name"] for c in m["checks"]][:3] == ["online", "nodes", "nodes match import"]
    assert neo.altered == [("catalog", m["newdb"])]
    assert out["baseline"][0]["taken"] is None           # read-only: not backed up by design


def test_failed_check_drops_and_moves_no_alias():
    neo = Neo(nodes=2)  # the import said 3
    _calls, run = run_admin()
    with pytest.raises(L.LoadError) as e:
        L.load_group(run, BackupRunner(), neo, MemStore(), LAYOUT, group())
    assert e.value.step == "checks" and neo.altered == [] and len(neo.dropped) == 1


def test_failed_build_names_the_step():
    neo = Neo()

    def boom(cmd, env=None):
        raise RuntimeError("import failed")
    with pytest.raises(L.LoadError) as e:
        L.load_group(boom, BackupRunner(), neo, MemStore(), LAYOUT, group())
    assert e.value.step == "build catalog" and neo.seeded == []


def test_reapply_access():
    class N(Neo):
        def resolve_physical(self, a):
            return f"{a}-1"
    neo = N()
    out = L.reapply_access(neo, group(mode="once"))
    assert out == [{"member": "catalog", "physical": "catalog-1", "access": "read-only"}]


# --- #108: rebuild lanes and the per-group lock ---------------------------------------------------

def test_lock_excludes_and_expires(monkeypatch):
    from neo4j_backup_core import locks

    class S(MemStore):
        def create_exclusive(self, key, text):
            if key in self.objects:
                return False
            self.objects[key] = text
            return True
    s = S()
    tok = locks.acquire(s, "cat", "load")
    with pytest.raises(locks.Locked, match="busy: load"):
        locks.acquire(s, "cat", "backup a")
    waits = []
    with pytest.raises(locks.Locked):
        locks.acquire(s, "cat", "backup a", wait=25, poll=10, sleep=waits.append)
    assert waits  # it polled before giving up
    locks.release(s, "cat", tok)
    with locks.group_lock(s, "cat", "backup a"):
        pass
    locks.acquire(s, "cat", "stuck", ttl=-1)            # an expired lock is cleared
    assert locks.acquire(s, "cat", "load")


def test_dagster_rebuild_schedule_requests_only_rebuild_groups(tmp_path):
    dg = pytest.importorskip("dagster")
    import json
    import os
    import subprocess
    import sys

    import yaml
    raw = yaml.safe_load(open("policies/demo.yaml"))
    raw["db_groups"].append({"id": "cat", "aliases": ["catalog"], "tier": "rebuild_nightly",
                             "load": {"mode": "rebuild", "writes": "read-only",
                                      "build": {"kind": "import", "input_type": "csv"}}})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    code = ("import json, dagster as dg; from neo4j_backup_dagster import definitions as D\n"
            "s = next(x for x in D.defs.schedules if x.name == 'rebuild_nightly_rebuild')\n"
            "with dg.instance_for_test() as i:\n"
            "    r = s.evaluate_tick(dg.build_schedule_context(instance=i, "
            "repository_def=D.defs.get_repository_def())).run_requests\n"
            "print(json.dumps([[x.tags, x.run_config] for x in r]))")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env={**os.environ, "NEO4J_PASSWORD": "x", "NEO4J_BACKUP_POLICY": str(f)})
    assert p.returncode == 0, p.stderr
    reqs = json.loads(p.stdout.splitlines()[-1])
    assert len(reqs) == 1
    tags, cfg = reqs[0]
    assert tags["neo4j_group"] == "cat" and cfg["ops"]["load_group_op"]["config"]["group_id"] == "cat"


# --- #109: kind: command builds ----------------------------------------------------------------

def cmd_group(**build):
    b = {"kind": "command", "command": ["rdf2csv"], "args": ["--in", "src"],
         "import_args": ["--nodes=nodes.csv"], "env": {"TOKEN": "secret:env:RDF_TOKEN",
                                                       "MODE": "fast"}, **build}
    return Policy.model_validate({"db_groups": [{"id": "cat", "aliases": ["catalog"],
                                                 "tier": "nightly", "load": {
                                                     "mode": "rebuild", "writes": "read-only",
                                                     "build": b}}],
                                  "tiers": TIERS}).group("cat")


def test_command_build_validation():
    with pytest.raises(ValueError, match="import_args"):
        cmd_group(import_args=[])
    with pytest.raises(ValueError, match="timeout"):
        cmd_group(timeout="soon")
    assert cmd_group(timeout="90m").load.build.timeout_seconds == 5400
    cmd_group(output="dump", import_args=[])


def test_command_build_runs_in_one_step_with_env_and_timeout(monkeypatch):
    monkeypatch.setenv("RDF_TOKEN", "t0k")
    neo, seen = Neo(), []

    def run(cmd, env=None, image=None):
        seen.append((cmd, env, image))
        return IMPORT_OUT
    out = L.load_group(run, BackupRunner(scratch_path="/scratch"), neo, MemStore(), LAYOUT,
                       cmd_group())
    cmd, env, image = seen[0]
    script = cmd[2]
    assert "timeout 7200 rdf2csv --in src" in script and "database import full" in script
    # relative import files are made absolute under the work dir (found on the stack: #109)
    assert f"--nodes={env['WORK_DIR']}/nodes.csv" in script
    assert "database dump" in script and "rm -rf" in script
    assert env["TOKEN"] == "t0k" and env["MODE"] == "fast"     # secret resolved
    assert env["SOURCE_URI"] == "s3://bucket/cat/_source/" and env["WORK_DIR"].startswith("/scratch/")
    assert image is None and out["members"][0]["import_counts"]["nodes"] == 3


def test_command_build_with_dump_output():
    script = BackupRunner().command_build_command(["make-dump"], "db", "/w", "s3://b/x/", None,
                                                  "dump", 60)[2]
    assert "test -s /w/db.dump" in script and "database load --from-path=/w" in script


def test_image_build_writes_to_output_uri_then_imports(monkeypatch):
    monkeypatch.setenv("RDF_TOKEN", "t0k")
    seen = []

    def run(cmd, env=None, image=None):
        seen.append((cmd, env, image))
        return IMPORT_OUT
    logged = []
    L.load_group(run, BackupRunner(), Neo(), MemStore(), LAYOUT,
                 cmd_group(command=[], image="registry/rdf:1"), log=logged.append)
    assert seen[0][2] == "registry/rdf:1" and seen[0][0] == ["--in", "src"]
    # the build pod's output goes to the run's log (#109; seen missing on k3d)
    assert any(x.startswith("build: ") for x in logged), logged
    out_uri = seen[0][1]["OUTPUT_URI"]
    assert f"--nodes={out_uri}nodes.csv" in seen[1][0][2]  # the import reads what it wrote


def test_failed_command_fails_before_any_seed():
    neo = Neo()

    def run(cmd, env=None, image=None):
        raise RuntimeError("exit 3")
    with pytest.raises(L.LoadError) as e:
        L.load_group(run, BackupRunner(), neo, MemStore(), LAYOUT, cmd_group())
    assert e.value.step == "build catalog" and neo.seeded == []


# --- #126: notice before the swap -----------------------------------------------------------------

def notice_group(**notice):
    g = group()
    from neo4j_backup_core.policy import Notice
    g.load.notice = Notice(**{"hook": "notify-me", "before": "15m", **notice})
    return g


def test_notice_calls_the_hook_then_waits(monkeypatch):
    from datetime import datetime, timezone
    calls, waits = [], []
    monkeypatch.setattr("neo4j_backup_core.cutover.call_hook",
                        lambda hook, payload, env_prefix: calls.append((hook, payload, env_prefix)))
    g = notice_group()
    plan = L.plan_load(Neo(), MemStore(), LAYOUT, g)
    st = L.notify(g, plan, sleep=waits.append,
                  now=datetime(2026, 11, 16, 1, 0, tzinfo=timezone.utc))
    hook, payload, prefix = calls[0]
    assert prefix == "NOTICE_" and payload["group"] == "cat" and payload["members"] == ["catalog"]
    assert payload["swap_at"] == "2026-11-16T01:15:00+00:00" and payload["loses"]
    assert waits == [900] and st["sent"] is True


def test_failed_notice_is_logged_unless_required(monkeypatch):
    def boom(*a, **k):
        raise ConnectionError("hook down")
    monkeypatch.setattr("neo4j_backup_core.cutover.call_hook", boom)
    plan = L.plan_load(Neo(), MemStore(), LAYOUT, notice_group())
    st = L.notify(notice_group(before="0m"), plan, sleep=lambda s: None)
    assert st["sent"] is False and "hook down" in st["error"]
    with pytest.raises(ops.OpError, match="notice.required"):
        L.notify(notice_group(required=True), plan, sleep=lambda s: None)


def test_notice_command_gets_env_vars(monkeypatch):
    import subprocess
    from neo4j_backup_core.cutover import call_hook
    seen = {}
    monkeypatch.setattr(subprocess, "run", lambda args, **kw: seen.update(args=args, env=kw["env"]))
    call_hook("notify --x", {"group": "cat", "members": ["a"], "tenant": None},
              env_prefix="NOTICE_")
    assert seen["args"] == ["notify", "--x"] and seen["env"]["NOTICE_GROUP"] == "cat"
    assert seen["env"]["NOTICE_MEMBERS"] == '["a"]' and seen["env"]["NOTICE_TENANT"] == ""
