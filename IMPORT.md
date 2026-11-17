# Bulk import: seed a database from raw data off-cluster

For CSV or Parquet data outside Neo4j. Run the import on a throwaway loader host,
and the ordinary [restore](RECOVERY.md) seeds the result. Terms: [GLOSSARY.md](GLOSSARY.md).
For scheduled loads, use a group's `load:` block instead ([Load data in bulk](workflows/load.md)).

`neo4j-admin database import full` writes an offline store, and the default route dumps it to a `.dump`.
A dump restores whole only (`seedRestoreUntil` is refused).
Your `--nodes` and `--relationships` arguments pass through. For Parquet add `--input-type=parquet`.

## Default route: `import --to-dump`

On the loader (through `RUNNER_EXEC_PREFIX` or a local `neo4j-admin`), before the store's first start:

```bash
neo4j-backup --json import --to-dump <group>/<member> loaddb \
  --nodes=Product=/data/products.csv --relationships=IN=/data/in.csv
# -> result.key: <group>/<slug>/_seed/<ts>/loaddb.dump   (result.uri is the seed URI)
```

The dump lands in the member's `_seed/` folder, and `show_chain` lists it under `loads`.
`BACKUP_UPLOAD=pipeline` uploads it with SSE headers. Then seed the group from it:

```bash
neo4j-backup --json restore <group> --from-dump <member>=<result.key> --dry-run   # then --confirm
neo4j-backup --json restore <group> --from-load --confirm   # every member's newest load
```

The restore copies privileges and takes the baseline full, as any restore does.
A group holding only loads seeds from the newest load by default. Then discard the loader.

## Export a group as dumps

`neo4j-backup export <group> [--until <iso>] --confirm` dumps each member to `_seed/`,
at any second a chain covers. The runner builds it from the chain locally and leaves the cluster untouched.

## Import into a fresh loader only

!!! warning
    Import into the default database of a loader whose server has yet to start.
    Each database on a running DBMS has a fixed store id.
    An import has another id, so the database stays offline.
    A fresh server adopts it on first boot.

## Native `.backup` route

A `.backup` only comes from an online backup, so this route needs a running Neo4j on the loader.
Backup connects to the loader's port 6362 (`NEO4J_BACKUP_SOURCE`).
Import and backup both honour `RUNNER_EXEC_PREFIX`, `SCRATCH_PATH` and `RUNNER_NEO4J_ADMIN`.

1. Provision a loader with a fresh Neo4j Enterprise, a data disk and scratch space. Set
    `server.backup.enabled=true`.

2. Import into the default database, then back it up into the target database's prefix:

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

     The loader sits outside the policy, so this is a direct `neo4j-admin database backup`.

3. Seed the group. The `.backup` is now a chain head at `<group>/<slug>/<physical>/`:

     ```bash
     neo4j-backup --json verify <group>
     neo4j-backup --json restore <group> --dry-run    # then --confirm
     ```

     The restore's baseline full starts the seeded database's own chain.

## `neo4j-backup import`

```
neo4j-backup import <database> [--] <neo4j-admin import args…>
```

Runs `neo4j-admin database import full <database> <args…>` through the runner.
The args pass through raw, and a leading `--` is dropped. It skips the `--confirm` guard.
Import runs from the CLI only. The operator owns the loader's lifecycle.

## Incremental import and the backup chain

`neo4j-admin database import incremental` adds bulk data to an existing database.
Its merge changes the store id, so take a full backup afterwards (`neo4j-backup backup <group> --kind FULL`).
A scheduled DIFF that meets a changed store id is skipped with `needs_full: true, reason: "store id changed"`.
Restores to before the import use the old chain. Times between the import and the new full lack a chain.
`restore --until` refuses them and lists the nearest windows.

### Running it

Run it on the server that hosts the database, as the user that runs Neo4j.
Files written as root leave the database offline.

1. Back up first. A failed merge can corrupt the database.
2. On a cluster, reduce the database to one primary and work on that server (see Neo4j's
    [incremental import](https://neo4j.com/docs/operations-manual/current/import/incremental-import/)).

3. `STOP DATABASE <db>`. Run `neo4j-admin database import incremental <db> --force
    --stage=prepare --nodes=...`. Then run `--stage=build` (the database may be online and
    read-only) and `--stage=merge` (stopped). Put the database name before `--nodes`. The id
    column needs the node key's label: `id:ID(P){label:P}`.

4. `START DATABASE <db>`, restore the topology on a cluster, then
    `neo4j-backup backup <group> --kind FULL`.
