# Bulk import: seed a database from raw data off-cluster

Sometimes the data doesn't exist in Neo4j yet — it's CSV (or Parquet) you need to load and then hand
to the fleet as a restorable backup. This is **episodic provisioning**, not a policy-driven cadence:
you run it by hand (or from an external pipeline) on **ephemeral hardware**, and most of it is
team-specific. What this project standardises is the **tail** — turning a built store into a native
`.backup` that the ordinary [restore path](RECOVERY.md) can seed anywhere.

## Why it's a runbook plus one thin command

`neo4j-admin database import full` produces an **offline store**, not a `.backup`. The only route to
a native `.backup` is an **online** backup of a running server, so this pattern needs a throwaway
Neo4j on the loader, and provisioning that is yours.

A `.dump` avoids that, and is the default route: `neo4j-admin database dump` works on the imported
store offline, without ever starting a server, and seed-from-URI accepts a dump when the URI names
the file (verified on 2026.05; a directory seed only uses backup chains). A dump restores whole
only: `seedRestoreUntil` is refused for it. The raw input is open and per-dataset — the
`--nodes` / `--relationships` mapping is your data model. So `neo4j-backup` **structures the calls;
it does not model your import args.**

## Default route: `import --to-dump`

```bash
# on the loader (RUNNER_EXEC_PREFIX / a local neo4j-admin), store never started:
neo4j-backup --json import --to-dump <group>/<member> loaddb \
  --nodes=Product=/data/products.csv --relationships=IN=/data/in.csv
# -> result.key: <group>/<slug>/_seed/<ts>/loaddb.dump   (result.uri is the seed URI)
```

One command imports into `loaddb` on the loader, dumps the offline store, and puts the dump in the
member's load folder (`_seed/`, #156), where the tool lists it apart from the backup chains
(`show_chain` returns it under `loads`). With `BACKUP_UPLOAD=pipeline` the dump goes through the
runner's staging folder and the tool's own upload (SSE headers); otherwise neo4j-admin writes it to
the store directly. Then seed the group from it:

```bash
neo4j-backup --json restore <group> --from-dump <member>=<result.key> --dry-run   # then --confirm
neo4j-backup --json restore <group> --from-load --confirm   # every member's newest load
```

The restore copies privileges and takes the baseline full backup, as any restore does.

**Outbound.** `neo4j-backup export <group> [--until <iso>] --confirm` gives a portable dump of
each member at any point a backup chain covers (or the latest backup), for a customer who wants
their data out, or a copy to load elsewhere. In the runner it restores the chain into a throwaway
local store (`neo4j-admin database restore --restore-until`), dumps it to the member's `_seed/`
folder and removes the store; the cluster is never touched (#160). `--restore-until` works to the
second. A group
whose members have no `.backup` yet (load-only) seeds from the newest load without either flag.
The imported store stays in the loader's data directory; throw the loader away, or remove the
store, afterwards. The `.backup` route below stays for anyone who wants a native backup.

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

**1. Source → store (team-owned).** CSV and Parquet both feed `neo4j-admin database import full`
directly: add `--input-type=parquet` for Parquet (verified on 2026.05 with no extra jars; Neo4j
introduced Parquet import as a public beta in 5.25). The `--nodes`/`--relationships` mapping is
yours.

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
loader's db name doesn't leak through. It is a restore point only: a backup chain belongs to one
database, so the seeded database can't add differential backups to the loader's backup. The restore
takes a full backup of each seeded database at the end (the baseline, on by default), and copies the
customer's privileges, as described in the [Operator guide](OPERATOR.md#restore-on-a-customers-request):

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

## Incremental import and the backup chain (#99)

`neo4j-admin database import incremental` adds bulk data to an existing database. Tested on
Neo4j **2026.05.0 Enterprise**, single server, block format, with
`orchestrator/experiment_incremental.py`:

| Step | Result |
|---|---|
| FULL, write, DIFF | normal chain (tx 1-7, then 8) |
| STOP, `import incremental` prepare, build, merge, START | the database has the imported nodes |
| DIFF after the import | **fails**: `Remote store and existing backup have different store ids` |
| FULL after the import | works, and starts a new chain in the same folder (different `storeIdHash`) |
| DIFF on that full | works |
| aggregate a copy of the folder, `database check` | aggregates the newest chain; check passes |
| seed the old chain until a time before the import | correct (the state at that time) |
| seed the new chain, newest or until a time after its full | correct |
| a time between the import and the new full | no chain covers it; `restore --until` refuses and lists the nearest windows (#114) |

So the merge rewrites the store and changes its store id, and the chain can't continue: **after
an incremental import, take a full backup** (`neo4j-backup backup <group> --kind FULL`). The
tool helps: a scheduled DIFF that meets a changed store id is skipped with
`needs_full: true, reason: "store id changed"` instead of failing, so the full lane (or you)
starts the new chain. Point-in-time restores to before the import still use the old chain.

### Running it

The import works on the stopped database's own store files, so it runs **on the server that
hosts the database**, not on the backup runner, and as the user that runs Neo4j. Files written as
root leave the database unable to start (it stays offline, with the path of a transaction log in
`statusMessage`).

1. Back up first (Neo4j recommends it: a failed merge can corrupt the database).
2. On a cluster, reduce the database to one primary and do the rest on that server; afterwards
   the other members copy the database from it as the seeder (Neo4j's
   [incremental import](https://neo4j.com/docs/operations-manual/current/import/incremental-import/)
   page). On a single server, skip this.
3. `STOP DATABASE <db>`. Run `neo4j-admin database import incremental <db> --force
   --stage=prepare --nodes=...`, then `--stage=build` (the database may be online and
   read-only for this stage), then `--stage=merge` (stopped). Put the database name before
   `--nodes`/`--relationships`, which take several values. The id column needs the node key's
   label: `id:ID(P){label:P}`.
4. `START DATABASE <db>`, restore the topology on a cluster, then
   `neo4j-backup backup <group> --kind FULL`.

## Non-goals

Provisioning, delivering params to the loader, monitoring, cleanup, modelling
the import args, and any policy/cadence integration — bulk import stays manual / external-pipeline
driven. There is no MCP `import` tool: the [MCP server](MCP.md) is operator-assist for the running
fleet (DR/status), not provisioning.
