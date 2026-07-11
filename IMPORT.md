# Bulk import: seed a database from raw data off-cluster

Sometimes the data doesn't exist in Neo4j yet — it's CSV (or Parquet) you need to load and then hand
to the fleet as a restorable backup. This is **episodic provisioning**, not a policy-driven cadence:
you run it by hand (or from an external pipeline) on **ephemeral hardware**, and most of it is
team-specific. What this project standardises is the **tail** — turning a built store into a native
`.backup` that the ordinary [restore path](RECOVERY.md) can seed anywhere.

## Why it's a runbook plus one thin command

`neo4j-admin database import full` produces an **offline store**, not a `.backup`. The only route to
a native `.backup` is an **online** backup of a running server (a `.dump` is the offline artifact,
and it doesn't fit seed-from-URI). So the pattern inherently needs a throwaway Neo4j on the loader,
and provisioning that is yours. The raw input is also open and per-dataset — the `--nodes` /
`--relationships` mapping is your data model. So `neo4j-backup` **structures the calls; it does not
model your import args.**

> **Import into the default database of a *fresh, never-started* loader — not a named/existing db.**
> On current Neo4j (validated on 2026.05), `neo4j-admin database import full` into an
> *already-registered* database — `CREATE DATABASE x` then import, or importing into a running DBMS
> — **quarantines** the store: the system database has already fixed a store-ID that the imported
> store doesn't match, and the database comes up offline. A loader whose server has never started
> adopts the imported store cleanly on first boot. `just import-smoke` exercises exactly this end to
> end (import → adopt → backup → seed).

## Execution model — two different hosts

- **Import runs on the loader**, where the DBMS data directory lives — `neo4j-admin database import
  full` writes the store into that data dir, offline, before the server starts.
- **Backup is a network client** to the loader's running server on `:6362`.

Both honour the runner seams: `RUNNER_EXEC_PREFIX` (run neo4j-admin on the loader), `SCRATCH_PATH`,
`RUNNER_NEO4J_ADMIN`, and (for backup) `NEO4J_BACKUP_SOURCE`.

## The pattern

**1. Source → store (team-owned).** CSV feeds `neo4j-admin database import full` directly; Parquet is
converted to CSV first by your pipeline. The `--nodes`/`--relationships` mapping is yours.

**2. Provision a fresh loader (team-owned).** Ephemeral hardware with a **fresh, never-started**
Neo4j Enterprise, a data disk for the store, and scratch for the backup. Enable the backup listener
(`server.backup.enabled=true`).

**3. Import, then back up (this ships).** On the loader, before the server ever starts, import into
the **default database**, then start it and back it up into the target physical's storage prefix:

```bash
# import raw data into the loader's default db, BEFORE first start (RUNNER_EXEC_PREFIX -> the loader)
neo4j-backup --json import neo4j -- --nodes=/data/nodes.csv --relationships=/data/rels.csv

# set the password (pre-start) and start Neo4j -> the default db adopts the imported store
# then online-back it up into the target physical's prefix (NEO4J_BACKUP_SOURCE -> the loader's :6362).
# mint the physical so it lands where restore looks:
physical=$(orchestrator/.venv/bin/python -c "from neo4j_backup_core import naming; print(naming.physical('acme-orders'))")
neo4j-admin database backup --from <loader>:6362 \
  --to-path "s3://$BUCKET/<group>/$(slug acme-orders)/$physical/" neo4j
```

The loader has no policy/aliases, so this last step is a direct `neo4j-admin database backup` of the
loader's default db into the target prefix (rather than `neo4j-backup backup <group>`, which resolves
a policy group's aliases on a normal source).

**4. Seed (the existing path).** The `.backup` is now an ordinary chain head at
`<group>/<slug>/<physical>/`; seed-from-URI restores its store into whatever target you name — the
loader's db name doesn't leak through:

```bash
neo4j-backup --json verify <group>
neo4j-backup --json restore <group> --dry-run    # then --confirm
```

## `neo4j-backup import`

```
neo4j-backup import <database> [--] <neo4j-admin import args…>
```

Runs `neo4j-admin database import full <database> <args…>` via the runner (the database comes first —
`--nodes`/`--relationships` are multi-value and would otherwise swallow a trailing database). `<args…>`
is a raw passthrough (an optional leading `--` separator is dropped). Honours `RUNNER_EXEC_PREFIX` /
`RUNNER_NEO4J_ADMIN`. Emits the [CLI contract](CLI-CONTRACT.md) envelope (`--json`) and exit codes.
It is not guarded (`--confirm` is not required) — it creates a *local* store on ephemeral hardware,
not a production or backup mutation.

Verify the exact `--nodes`/`--relationships`/option surface against your deployed Neo4j version's
`neo4j-admin database import full` docs. The local check is `just import-smoke`.

## Non-goals

Provisioning, delivering params to the loader, monitoring, cleanup, Parquet→CSV conversion, modelling
the import args, and any policy/cadence integration — bulk import stays manual / external-pipeline
driven. There is no MCP `import` tool: the [MCP server](MCP.md) is operator-assist for the running
fleet (DR/status), not provisioning.
