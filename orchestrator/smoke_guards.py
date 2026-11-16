"""Guards and checks from the v0.6.0 audit (#216), on the local stack:

1. #217: the Dagster backup asset refuses a group fenced by `restore --to-cluster`.
2. #226: after an incremental import changes a database's store id, the next diff is skipped
   and the Dagster diff lane asks for a FULL; after the full it asks for diffs again.
3. #221: what `CREATE DATABASE … WAIT n SECONDS` returns per server on this Neo4j.
4. #230: one client reuses its Bolt driver (timing of 200 statements).
5. #233: Dagster's status_job agrees with the CLI's `status`.

Prereqs: `just fresh` (and some backups, e.g. `just cli-smoke`).
    orchestrator/.venv/bin/python orchestrator/smoke_guards.py
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml"]
EXEC = [*COMPOSE, "exec", "-T", "runner"]
AUTH = ("neo4j", "devpassword")


def policy() -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    raw["db_groups"] += [{"id": "fencegrp", "tier": "gold", "aliases": ["fence-x"]},
                         {"id": "incrgrp", "tier": "gold", "aliases": ["incrsm"]}]
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


POL = policy()
ENV = {"NEO4J_BOLT_URI": "neo4j://localhost:7687", "NEO4J_PASSWORD": AUTH[1],
       "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
       "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
       "BACKUP_BUCKET": "neo4j-backups", "SCRATCH_PATH": "/scratch",
       "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "BACKUP_UPLOAD": "admin",
       "NEO4J_BACKUP_POLICY": POL, "POLICY_CACHE_TTL": "0"}
os.environ.update(ENV)
os.chdir(REPO)

import dagster as dg  # noqa: E402
from neo4j import GraphDatabase  # noqa: E402

from neo4j_backup_core import ops  # noqa: E402
from neo4j_backup_core.clients import Neo4jClient, object_store  # noqa: E402
from neo4j_backup_dagster import definitions as D  # noqa: E402
from neo4j_backup_dagster.resources import (Neo4jResource, ObjectStoreResource,  # noqa: E402
                                            RunnerResource)

RES = {"neo4j": Neo4jResource(uri="neo4j://localhost:7687", user="neo4j", password=AUTH[1]),
       "store": ObjectStoreResource(bucket="neo4j-backups", endpoint_url="http://localhost:9000",
                                    region="us-east-1"),
       "runner": RunnerResource(backup_source="neo4j:6362", scratch_path="/scratch",
                                pagecache="512M", heap_size="2G", exec_prefix=EXEC),
       "pipes_subprocess_client": dg.PipesSubprocessClient()}


def sysq(cypher):
    with GraphDatabase.driver("neo4j://localhost:7687", auth=AUTH) as d, \
            d.session(database="system") as s:
        return [r.data() for r in s.run(cypher)]


def cli(*args):
    p = subprocess.run([os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"),
                        "--json", *args], env={**os.environ}, capture_output=True, text=True)
    return p.returncode, json.loads(p.stdout)


def fence_check():
    store = object_store("neo4j-backups", "http://localhost:9000", "us-east-1")
    ops.fence(store, "fencegrp", "default", "west")
    try:
        with dg.instance_for_test() as inst:
            inst.add_dynamic_partitions(D.targets.name, ["fencegrp/fence-x"])
            res = dg.materialize([D.backup], partition_key="fencegrp/fence-x", resources=RES,
                                 instance=inst, raise_on_error=False,
                                 run_config={"ops": {"backup": {"config": {"kind": "FULL"}}}})
            msgs = " ".join(ev.step_failure_data.error.message
                            for ev in res.get_step_failure_events())
        assert not res.success and "cluster: west" in msgs, msgs[-500:]
        print("== 1. Dagster backup asset refused the fenced group (moved to west) ==")
    finally:
        store.delete_keys(["_state/fencegrp.json"])


def store_id_check():
    sysq("DROP DATABASE incrsm IF EXISTS WAIT")
    sysq("CREATE DATABASE incrsm WAIT")
    with GraphDatabase.driver("neo4j://localhost:7687", auth=AUTH) as d, \
            d.session(database="incrsm") as s:
        s.run("CREATE CONSTRAINT p_id IF NOT EXISTS FOR (p:P) REQUIRE p.id IS NODE KEY").consume()
        s.run("UNWIND range(1, 3) AS i CREATE (:P {id: i})").consume()
    code, out = cli("backup", "incrgrp", "--kind", "FULL")
    assert code == 0, out
    subprocess.run([*COMPOSE, "exec", "-T", "--user", "neo4j", "neo4j", "sh", "-c",
                    "printf 'id:ID(P){label:P},:LABEL\\n900,P\\n901,P\\n' > /data/inc-sm.csv"],
                   check=True)
    sysq("STOP DATABASE incrsm WAIT")
    for stage in ("prepare", "build", "merge"):
        subprocess.run([*COMPOSE, "exec", "-T", "--user", "neo4j", "neo4j", "neo4j-admin",
                        "database", "import", "incremental", "incrsm", "--force",
                        f"--stage={stage}", "--nodes=P=/data/inc-sm.csv"], check=True,
                       capture_output=True)
    sysq("START DATABASE incrsm WAIT")
    code, out = cli("backup", "incrgrp", "--kind", "DIFF")
    b = out["result"]["backups"][0]
    assert code == 0 and b["skipped"] and b.get("reason") == "store id changed", out
    reqs = D.diff_lane_requests(["incrgrp/incrsm"], RES["neo4j"], RES["store"],
                                log=print, tick="t1")
    assert reqs[0].tags["backup_kind"] == "full", reqs[0].tags
    code, out = cli("backup", "incrgrp", "--kind", "FULL")
    assert code == 0 and out["result"]["backups"][0]["taken"] == "full", out
    reqs = D.diff_lane_requests(["incrgrp/incrsm"], RES["neo4j"], RES["store"],
                                log=print, tick="t2")
    assert reqs[0].tags["backup_kind"] == "diff", reqs[0].tags
    print("== 2. store-id change: diff skipped, the diff lane asked for a FULL; after it, diffs "
          "again ==")
    sysq("DROP DATABASE incrsm IF EXISTS WAIT")


def wait_rows():
    sysq("DROP DATABASE waitprobe IF EXISTS WAIT")
    rows25 = sysq("CREATE DATABASE waitprobe WAIT 30 SECONDS")
    sysq("DROP DATABASE waitprobe IF EXISTS WAIT")
    rows5 = sysq("CYPHER 5 CREATE DATABASE waitprobe WAIT 30 SECONDS")
    sysq("DROP DATABASE waitprobe IF EXISTS WAIT")
    assert rows5 and {"address", "state", "message", "success"} <= set(rows5[0]), rows5
    print(f"== 3. CREATE DATABASE … WAIT returns {rows25} under Cypher 25 and {rows5} under "
          f"CYPHER 5; the online check before the swap guards both ==")


def driver_reuse():
    c = Neo4jClient("neo4j://localhost:7687", "neo4j", AUTH[1])
    t = time.monotonic()
    for _ in range(200):
        c.alias_target("acme-orders")
    reused = time.monotonic() - t
    t = time.monotonic()
    for _ in range(50):
        Neo4jClient("neo4j://localhost:7687", "neo4j", AUTH[1]).alias_target("acme-orders")
    fresh = (time.monotonic() - t) * 4
    print(f"== 4. 200 statements: {reused:.2f}s on one driver; ~{fresh:.2f}s with a driver each ==")


def status_check():
    res = D.status_job.execute_in_process(resources={"store": RES["store"]}, raise_on_error=False)
    code, out = cli("status")
    stale = out["result"]["stale"]
    assert res.success == (not stale), (res.success, stale)
    print(f"== 5. status_job {'passed' if res.success else 'failed'}; CLI status stale: "
          f"{stale or 'none'} ==")


def main():
    fence_check()
    store_id_check()
    wait_rows()
    driver_reuse()
    status_check()
    print("PASS: guards and checks (#217, #226, #221, #230, #233)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        os.unlink(POL)
