# Recovery

Every restore (CLI, MCP, Dagster or Airflow) runs this Cypher, which also works by hand.
A restore seeds a new database from a backup, waits for it to come online, then points the alias at it.
Terms: [GLOSSARY.md](GLOSSARY.md).
Runbook: [Restore on request](workflows/restore.md).
Diagram: [restore-cutover](diagrams/restore-cutover.dot).

## The three recovery modes

All three use `CREATE DATABASE … OPTIONS { seedURI }`. For a differential, Neo4j finds the rest of the chain
in the same folder. Region and endpoint come from the server's `AWS_REGION` and `AWS_ENDPOINT_URL_S3`.

| Mode | `seedURI` points at | Extra option | Recovers to |
|---|---|---|---|
| Full | a full `.backup` | - | the full itself |
| Differential (HEAD) | a differential `.backup` | - | the newest backed-up state |
| PITR | a differential `.backup` | `seedRestoreUntil: datetime(T)` | the chain replayed to T |

### 1. Restore from a full

```cypher
CREATE DATABASE `orders-20260629t120000`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<full>.backup' } WAIT
```

### 2. Restore from a differential chain (HEAD)

```cypher
CREATE DATABASE `orders-20260629t120500`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup' } WAIT
```

### 3. Point-in-time recovery (PITR)

PITR replays the chain to a chosen instant. `seedRestoreUntil` takes a timestamp or a transaction id.
A lone full fails with "can only be fully restored".

```cypher
CREATE DATABASE `orders-20260629t120900`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup',
            seedRestoreUntil: datetime('2026-06-29T11:00:00Z') } WAIT
```

### Finish a restore by hand

1. Wait until the new database is online.
2. Copy privileges, which Neo4j stores per database. Rerun each `SHOW PRIVILEGES AS COMMANDS` line
    naming the old database in `ON GRAPH` or `ON DATABASE`, with the new name.

3. Cut over: `ALTER ALIAS \`orders\` SET DATABASE TARGET \`orders-<ts>\``.
4. Take a full backup of the group: `neo4j-backup backup <group> --kind FULL`.

To roll back, point the alias at the previous database. Drop the old one after a soak period.

### Settings that change the command

`SEED_CYPHER_VERSION=5` emits `CYPHER 5 CREATE DATABASE … OPTIONS { seedURI: …, existingData: 'use' }`.
Cypher 5 requires `existingData` and Cypher 25 (the default) deprecates it. Match the cluster's Cypher version.

`topology:` in the group ([POLICY.md](POLICY.md)) adds a `TOPOLOGY` clause instead of the DBMS default.
It is an error on a single server. Change it and restore to reshape a database.

```cypher
CREATE DATABASE `orders-20260629t120000`
  TOPOLOGY 3 PRIMARIES 0 SECONDARIES
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<full>.backup' } WAIT
```

`CUTOVER_STRATEGY=external` with `CUTOVER_HOOK=<url|command>` is for apps routed through a proxy or DNS.
The tool calls the hook with the new and old names. The hook owns the alias and rollback.

## By-name restore (alias-free)

A group with `restore_mode: by-name` lists `databases:` and restores each by its own name.

| Case | Statements |
|---|---|
| Create if absent (default) | `CREATE DATABASE <name> OPTIONS { seedURI: … }`; existing databases stay |
| Replace (`--replace`) | `DROP DATABASE <name>`, then `CREATE … seedURI` |

Replace leaves a gap between drop and create, so run `verify` first. Dagster checks the group before dropping.

## Seed from a local file (`file://`)

With `FileSeedProvider` in `dbms.databases.seed_from_uri_providers`, `seedURI` accepts an absolute server path.

```cypher
CREATE DATABASE foo OPTIONS { seedURI: 'file:/seed/foo.backup' }
```

## In the pipeline

`neo4j-backup restore <group>` does this for every member:

- By default, the group goes back to the newest instant every member covers.
- With `--until T`, each member seeds from the chain covering T, with `seedRestoreUntil` T.
- Every new database must be online everywhere within the group's `rto_minutes`.
  Then privileges are copied and aliases moved, each move recorded for `rollback`.

Locally:

```bash
just restore demo                          # newest backup of each alias
just restore demo "2026-06-29T11:00:00Z"   # to a point in time
just demo-pitr                             # build a full + diff chain, restore into it
```

## Users, roles, privileges and aliases

These live in `system`, and seeds create user databases only. The policy's `dbms:` crons back them up
as a Cypher export, a binary backup, or both.

| | Metadata export (default) | Binary `system` backup |
|---|---|---|
| Restored with | Cypher over Bolt, cluster running | `neo4j-admin` on the servers, Neo4j stopped |
| Native passwords | lost; users get a placeholder | kept |
| Runs as | a job or DAG | a runbook (`bootstrap/restore_system.sh`) |

SSO or LDAP users need only the export. Native passwords also need the binary backup.

### Metadata export

Dagster's `metadata_export` job or Airflow's `neo4j_dbms_metadata_<cluster>` DAG writes
`_dbms/metadata-<ts>.cypher`. Replay it with `neo4j-backup metadata restore [--key <k>]`
(or `metadata_restore` / `neo4j_metadata_restore`). It creates only missing objects, so reruns are safe.

The export leaves out:

- Native passwords. Users come back with a random password and `CHANGE REQUIRED`.
- Remote alias credentials. Those aliases get a `<<SUPPLY>>` placeholder and are skipped.
- Alias targets newer than the export. In a full recovery the data restores move them.

### Binary `system` backup

Dagster's `system_backup` job or Airflow's `neo4j_dbms_system_<cluster>` DAG backs up `system`.
It writes a full into `_dbms/system/` and keeps the newest `dbms.keep_system` (default 14).

Seeds skip `system`, and it stops only with the server, so restoring it needs Neo4j stopped:

1. Stop Neo4j on the server.
2. Restore from the bucket:

     ```bash
     neo4j-admin database restore system --from-path=s3://<bucket>/<prefix>/_dbms/system/<newest>.backup --overwrite-destination=true
     ```

3. Start Neo4j.
4. Run the `restore_metadata.cypher` the restore wrote against `system`.

On a cluster, restore one server and let the others copy from it, or restore each.
`just restore-system` runs these steps on the demo server.

### Order for a full recovery

1. Rebuild the cluster.
2. Restore users, roles and privileges.
3. Restore each group's databases.

Add the new cluster to the policy's `clusters:`.
Then `neo4j-backup restore --from-cluster <lost> --to-cluster <new>` does steps 2 and 3 in one plan. See [Recover a lost cluster](workflows/recover.md).

## Caveats

- A point in time is reachable only if differentials cover it, so `diff_cron` sets the restore granularity.
- Aggregation removes point-in-time restore inside the chain, so `aggregate_cron` is unset by default.
- Seeds over http or ftp, and whole-server restores, need `neo4j-admin database restore
  --restore-until` on the server.
