"""Agentless logical capture/replay of the DBMS metadata layer — users, roles, role
memberships, privileges, and database aliases — rendered as replayable Cypher over Bolt.

Closes the agentless gap for the `system` layer (DESIGN §3/§13): seed-from-URI cannot
target `system`, and an exact binary `system` restore is offline + node-local (path B).
Replay reconstitutes the security + alias state on a fresh cluster purely over Bolt.

One honest limitation, verified against the server: Cypher does not expose native password
hashes — `SHOW USERS WITH AUTH` redacts the secret to `***`, and raw reads of the system
store are rejected. So users are recreated with a random placeholder + `CHANGE REQUIRED`;
exact passwords need the binary `system` backup. SSO/LDAP users carry no local secret, so
nothing is lost there. Remote-alias driver credentials are likewise not returned and must
be supplied out of band (rendered as a `<<SUPPLY>>` placeholder).
"""

from __future__ import annotations

import secrets

# Built-in roles exist on a fresh DBMS; don't re-CREATE them (PUBLIC can't be created at
# all). Their privileges/memberships still replay fine via the statements below.
BUILTIN_ROLES = {"PUBLIC", "admin", "architect", "editor", "publisher", "reader"}


def capture(neo) -> dict:
    """Snapshot the metadata layer via administrative SHOW commands (run against `system`)."""
    users = [
        {"name": r["user"], "suspended": r["suspended"], "home": r["home"]}
        for r in neo.run_system(
            "SHOW USERS YIELD user, suspended, home RETURN user, suspended, home"
        )
    ]
    roles = [r["role"] for r in neo.run_system("SHOW ROLES YIELD role RETURN role")]
    memberships = [
        (r["role"], r["member"])
        for r in neo.run_system(
            "SHOW ROLES WITH USERS YIELD role, member RETURN role, member"
        )
        if r["member"]
    ]
    privileges = [
        r["command"]
        for r in neo.run_system(
            "SHOW PRIVILEGES AS COMMANDS YIELD command RETURN command"
        )
    ]
    aliases = [
        dict(r)
        for r in neo.run_system(
            "SHOW ALIASES FOR DATABASE YIELD name, composite, database, location, url, user "
            "RETURN name, composite, database, location, url, user"
        )
    ]
    return {
        "users": users,
        "roles": roles,
        "memberships": memberships,
        "privileges": privileges,
        "aliases": aliases,
    }


def _bt(name: str) -> str:
    """Backtick-quote an identifier, escaping embedded backticks (Neo4j's own escaping)."""
    return "`" + str(name).replace("`", "``") + "`"


def render(snapshot: dict, ts: str = "") -> str:
    """Render the snapshot as a replayable Cypher script — one statement per line, comments
    with `//`. Statement order is replay-safe: roles → users → memberships → privileges →
    aliases."""
    lines: list[str] = [
        "// Neo4j DBMS metadata — replayable Cypher (users, roles, privileges, aliases).",
        f"// Captured: {ts or 'n/a'}. Replay against `system` over Bolt.",
        "// NOTE: native passwords are NOT exported (Cypher redacts them); users are",
        "//       recreated with a random placeholder + CHANGE REQUIRED — reset post-restore.",
        "",
        "// --- roles ---",
    ]
    for role in snapshot["roles"]:
        if role not in BUILTIN_ROLES:
            lines.append(f"CREATE ROLE {_bt(role)} IF NOT EXISTS")

    lines += ["", "// --- users (password reset on replay) ---"]
    for u in snapshot["users"]:
        stmt = (
            f"CREATE USER {_bt(u['name'])} IF NOT EXISTS "
            f"SET PASSWORD '{secrets.token_urlsafe(18)}' CHANGE REQUIRED"
        )
        if u.get("suspended"):
            stmt += " SET STATUS SUSPENDED"
        if u.get("home"):
            stmt += f" SET HOME DATABASE {_bt(u['home'])}"
        lines.append(stmt)

    lines += ["", "// --- role memberships ---"]
    for role, member in snapshot["memberships"]:
        if role != "PUBLIC":  # PUBLIC is granted implicitly
            lines.append(f"GRANT ROLE {_bt(role)} TO {_bt(member)}")

    lines += ["", "// --- privileges ---"]
    lines += list(snapshot["privileges"])  # already replayable GRANT/DENY command strings

    lines += ["", "// --- aliases ---"]
    for a in snapshot["aliases"]:
        name = _bt(a["name"])
        if a.get("composite"):
            name = f"{_bt(a['composite'])}.{name}"
        if a.get("location") == "remote":
            lines.append(
                f"CREATE ALIAS {name} IF NOT EXISTS FOR DATABASE {_bt(a['database'])} "
                f"AT '{a['url']}' USER {_bt(a.get('user') or 'neo4j')} PASSWORD '<<SUPPLY>>'"
            )
        else:
            lines.append(
                f"CREATE ALIAS {name} IF NOT EXISTS FOR DATABASE {_bt(a['database'])}"
            )

    return "\n".join(lines) + "\n"


def statements(cypher_text: str) -> list[str]:
    """Executable statements from a rendered script (drop blanks + `//` comments)."""
    out = []
    for line in cypher_text.splitlines():
        s = line.strip()
        if s and not s.startswith("//"):
            out.append(s)
    return out


def replay(neo, cypher_text: str) -> dict:
    """Run each statement against `system`. Remote-alias lines needing a supplied secret
    (`<<SUPPLY>>`) are skipped and reported rather than failing the whole replay."""
    applied, skipped = 0, []
    for stmt in statements(cypher_text):
        if "<<SUPPLY>>" in stmt:
            skipped.append(stmt)
            continue
        neo.run_system(stmt)
        applied += 1
    return {"applied": applied, "skipped": skipped}
