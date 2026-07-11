"""Validate the agentless metadata export (#14) end to end against the local stack:
capture users/roles/privileges/aliases -> render Cypher -> persist to the object store ->
drop the fixtures -> replay from the stored artifact -> assert the security + alias state
came back. Pure core (no Dagster/Airflow); restore is pure Cypher over Bolt.

    orchestrator/.venv/bin/python orchestrator/smoke_metadata.py
"""

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

from neo4j_backup_core import metadata, naming, paths
from neo4j_backup_core.clients import Neo4jClient, object_store

ROLE, USER, ALIAS = "t_meta_role", "t_meta_user", "t-meta-alias"


def main() -> None:
    neo = Neo4jClient("neo4j://localhost:7687", "neo4j", "devpassword")
    store = object_store("neo4j-backups", "http://localhost:9000", "us-east-1")

    target = neo.alias_target("acme-orders")  # an existing physical to point the alias at
    assert target, "alias acme-orders has no target — run `just bootstrap` first"

    def drop_fixtures():
        for stmt in (
            f"DROP ALIAS `{ALIAS}` IF EXISTS FOR DATABASE",
            f"DROP USER `{USER}` IF EXISTS",
            f"DROP ROLE `{ROLE}` IF EXISTS",
        ):
            neo.run_system(stmt)

    drop_fixtures()  # clean slate
    key = None
    try:
        print("== create fixtures (role, user, membership, privilege, alias) ==")
        neo.run_system(f"CREATE ROLE `{ROLE}`")
        neo.run_system(f"GRANT ACCESS ON DATABASE * TO `{ROLE}`")
        neo.run_system(f"CREATE USER `{USER}` SET PASSWORD 'initialPass123' CHANGE NOT REQUIRED")
        neo.run_system(f"GRANT ROLE `{ROLE}` TO `{USER}`")
        neo.run_system(f"CREATE ALIAS `{ALIAS}` FOR DATABASE `{target}`")

        print("== capture -> render -> store ==")
        ts = naming.ts()
        snap = metadata.capture(neo)
        assert ROLE in snap["roles"] and any(u["name"] == USER for u in snap["users"])
        assert (ROLE, USER) in snap["memberships"]
        assert any(a["name"] == ALIAS for a in snap["aliases"])
        cypher = metadata.render(snap, ts=ts)
        key = paths.metadata_key(ts)
        store.put_text(key, cypher)
        assert store.latest_text_key(paths.metadata_prefix()) == key, "artifact not listed"
        print(f"   stored {key} ({len(cypher)} bytes); fetched back == stored:",
              store.get_text(key) == cypher)

        print("== drop fixtures, then replay from the stored artifact ==")
        drop_fixtures()
        assert ROLE not in [r["role"] for r in neo.run_system("SHOW ROLES YIELD role")]
        result = metadata.replay(neo, store.get_text(key))
        print(f"   replayed {result['applied']} statements, skipped {len(result['skipped'])}")

        print("== assert security + alias state restored ==")
        roles = [r["role"] for r in neo.run_system("SHOW ROLES YIELD role")]
        users = [r["user"] for r in neo.run_system("SHOW USERS YIELD user")]
        aliases = [a["name"] for a in neo.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name RETURN name")]
        members = [(r["role"], r["member"]) for r in neo.run_system(
            "SHOW ROLES WITH USERS YIELD role, member RETURN role, member")]
        privs = [r["command"] for r in neo.run_system(
            "SHOW PRIVILEGES AS COMMANDS YIELD command RETURN command")]
        assert ROLE in roles, "role not restored"
        assert USER in users, "user not restored"
        assert ALIAS in aliases, "alias not restored"
        assert (ROLE, USER) in members, "role membership not restored"
        assert any(ROLE in c and "ACCESS" in c for c in privs), "privilege not restored"
        print("   role, user, membership, privilege, alias all restored")
        print("PASS: metadata capture -> store -> replay round-trips the DBMS metadata layer")
    finally:
        drop_fixtures()
        if key:
            store.delete_keys([key])


if __name__ == "__main__":
    main()
