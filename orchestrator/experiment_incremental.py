"""#99 experiment: does an incremental import break the differential backup chain?

On the local stack (Neo4j Enterprise, block format):
  FULL -> write node 4 (T1) -> DIFF -> STOP, `import incremental` prepare/build/merge (nodes
  100-102), START -> write node 5 (T2) -> DIFF -> aggregate + check a copy of the chain ->
  seed at T1, at T2 and at the newest artifact.

The import stages run inside the Neo4j container (they need the stopped database's own store);
backups run in the runner, as the tool does. Prints what happened at every step; the result is
recorded in IMPORT.md.

Prereqs: `just fresh`.   orchestrator/.venv/bin/python orchestrator/experiment_incremental.py
"""

import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

from neo4j import GraphDatabase

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml"]
DB, PREFIX = "incrdb", "s3://neo4j-backups/exp99/incrdb/"
AUTH = ("neo4j", "devpassword")
D = GraphDatabase.driver("neo4j://localhost:7687", auth=AUTH)


def sysq(q):
    with D.session(database="system") as s:
        return s.run(q).data()


def count(db):
    with D.session(database=db) as s:
        return s.run("MATCH (p:P) RETURN count(p) AS c").single()["c"]


def write(db, i):
    with D.session(database=db) as s:
        s.run("CREATE (:P {id: $i})", i=i).consume()


def sh(service, cmd, check=False):
    # in the Neo4j container, as the `neo4j` user: files the import writes as root can't be
    # opened by the server afterwards (the database stays offline)
    user = ["--user", "neo4j"] if service == "neo4j" else []
    p = subprocess.run([*COMPOSE, "exec", "-T", *user, service, "sh", "-c", cmd], cwd=REPO,
                       capture_output=True, text=True)
    tail = (p.stdout + p.stderr).strip().splitlines()[-4:]
    print(f"   [{service}] exit {p.returncode}: {' | '.join(tail)[:400]}")
    if check and p.returncode:
        raise SystemExit(f"step failed: {cmd}")
    return p.returncode


def backup(kind):
    return sh("runner", f"neo4j-admin database backup --from=neo4j:6362 --to-path={PREFIX} "
                        f"--temp-path=/scratch --pagecache=256M --type={kind} {DB}")


def server_time():
    with D.session(database="neo4j") as s:
        now = s.run("RETURN toString(datetime()) AS t").single()["t"][:19]
    t = datetime.fromisoformat(now) + timedelta(seconds=1)
    while True:
        with D.session(database="neo4j") as s:
            if s.run("RETURN datetime() > datetime($t) AS ok",
                     t=t.strftime("%Y-%m-%dT%H:%M:%SZ")).single()["ok"]:
                return t.strftime("%Y-%m-%dT%H:%M:%SZ")
        time.sleep(0.2)


def inspect():
    import json
    p = subprocess.run([*COMPOSE, "exec", "-T", "runner", "neo4j-admin", "backup", "inspect",
                        "--format=JSON", PREFIX], cwd=REPO, capture_output=True, text=True)
    line = next(x for x in p.stdout.splitlines() if x.startswith("["))
    for a in sorted(json.loads(line), key=lambda a: a["time"]):
        print(f"   {a['uri'].rsplit('/', 1)[1]}: full={a['full']} "
              f"tx {a['lowestTransaction']}-{a['highestTransaction']} "
              f"storeIdHash={a['storeIdHash']}")


def artifacts():
    out = subprocess.run([*COMPOSE, "run", "--rm", "-T", "awscli", "s3", "ls",
                          "s3://neo4j-backups/exp99/incrdb/"], cwd=REPO, capture_output=True,
                         text=True).stdout.split()
    return sorted(k for k in out if k.endswith(".backup"))


def seed(name, artifact, until=None):
    sysq(f"DROP DATABASE `{name}` IF EXISTS WAIT")
    opts = f"seedURI: '{PREFIX}{artifact}'" + (f", seedRestoreUntil: datetime('{until}')"
                                               if until else "")
    try:
        sysq(f"CREATE DATABASE `{name}` OPTIONS {{{opts}}} WAIT")
        n = count(name)
        print(f"   seed from {artifact} until {until}: {n} nodes")
        return n
    except Exception as e:  # noqa: BLE001
        print(f"   seed from {artifact} until {until}: FAILED {str(e).splitlines()[0][:300]}")
        return None
    finally:
        sysq(f"DROP DATABASE `{name}` IF EXISTS WAIT")


def main():
    os.chdir(REPO)
    print(sysq("CALL dbms.components() YIELD versions RETURN versions"))
    sh("runner", "rm -rf /tmp/x")  # noop warm-up
    subprocess.run([*COMPOSE, "run", "--rm", "-T", "awscli", "s3", "rm", "--recursive",
                    "s3://neo4j-backups/exp99/"], cwd=REPO, capture_output=True)
    sysq(f"DROP DATABASE {DB} IF EXISTS WAIT")
    sysq(f"CREATE DATABASE {DB} WAIT")
    with D.session(database=DB) as s:
        s.run("CREATE CONSTRAINT p_id IF NOT EXISTS FOR (p:P) REQUIRE p.id IS NODE KEY").consume()
        s.run("UNWIND range(1,3) AS i CREATE (:P {id: i})").consume()
    print(f"== start: {count(DB)} nodes ==")
    print("== 1. FULL ==")
    backup("FULL")
    write(DB, 4)
    t1 = server_time()
    print(f"== 2. node 4 written, T1={t1}; DIFF ==")
    backup("DIFF")
    print("== 3. STOP, import incremental (prepare, build, merge), START ==")
    sh("neo4j", "printf 'id:ID(P){label:P},:LABEL\\n100,P\\n101,P\\n102,P\\n' > /data/inc.csv")
    sysq(f"STOP DATABASE {DB} WAIT")
    for stage in ("prepare", "build", "merge"):
        # the database name first: --nodes takes several values and would swallow it
        sh("neo4j", f"neo4j-admin database import incremental {DB} --force --stage={stage} "
                    f"--nodes=P=/data/inc.csv", check=True)
    sysq(f"START DATABASE {DB} WAIT")
    print(f"   after import: {count(DB)} nodes")
    write(DB, 5)
    t2 = server_time()
    print(f"== 4. node 5 written, T2={t2}; DIFF after the import ==")
    old_chain = artifacts()
    rc = backup("DIFF")
    print(f"   DIFF after import: exit {rc}")
    print("== 5. FULL after the import (a new chain), node 6, DIFF ==")
    print(f"   FULL after import: exit {backup('FULL')}")
    write(DB, 6)
    t3 = server_time()
    print(f"   DIFF on the new full: exit {backup('DIFF')}; T3={t3}")
    inspect()
    print("== 6. aggregate a copy of the folder (both chains), then check ==")
    from neo4j_backup_core.clients import object_store
    store = object_store("neo4j-backups", "http://localhost:9000", "us-east-1")
    store.delete_prefix("exp99-copy/")
    store.copy_prefix("exp99/incrdb/", "exp99-copy/incrdb/")
    sh("runner", "neo4j-admin backup aggregate --from-path=s3://neo4j-backups/exp99-copy/incrdb/ "
                 f"--temp-path=/scratch {DB}")
    sh("runner", "neo4j-admin database check --from-path=s3://neo4j-backups/exp99-copy/incrdb/ "
                 f"--temp-path=/scratch {DB}; echo check-exit=$?")
    print("== 7. seed before the import, in the gap, after the new full ==")
    new_chain = [a for a in artifacts() if a not in old_chain]
    seed("exp99t1", old_chain[-1], t1)
    seed("exp99t2old", old_chain[-1], t2)
    seed("exp99t2new", new_chain[-1], t2)
    seed("exp99t3", new_chain[-1], t3)
    seed("exp99latest", new_chain[-1])
    print("== done ==")


if __name__ == "__main__":
    try:
        main()
    finally:
        D.close()
