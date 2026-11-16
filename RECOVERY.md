# Recovery

This page is the Cypher a restore runs, for understanding it and for recovering by hand. The
runbook for a customer's request (restore points, preview, `--ticket`, the baseline, reporting
back, `rollback`) is in [OPERATOR.md](OPERATOR.md#restore-on-a-customers-request); the CLI,
the MCP server, Dagster's `restore_group` job and Airflow's `neo4j_restore` DAG all run the
statements below.

A restore creates a new database seeded from a backup (`CREATE DATABASE … OPTIONS { seedURI }`),
checks it is online, then points the alias at it (`ALTER ALIAS`). The Neo4j servers fetch the
seed themselves; nothing runs on them. Diagram: [restore-cutover](diagrams/restore-cutover.dot).

The seed can be a full or a differential backup. Point `seedURI` at one `.backup` file; for a
differential, Neo4j finds its full and the diffs in between in the same folder and applies them.

Privileges are stored against the database, not the alias. The tool's restore copies them to
the new database before the switch. By hand, copy them yourself before `ALTER ALIAS`: run each
command from `SHOW PRIVILEGES AS COMMANDS` whose `ON GRAPH` / `ON DATABASE` names the old
database, with the new name in its place. Afterwards take a full backup of the restored group
(`neo4j-backup backup <group> --kind FULL`); the tool's restore does that itself.

## The three recovery modes

All three are the same `CREATE DATABASE … seedURI` command; they differ only in which
artifact you point at and whether you add `seedRestoreUntil`. Region and endpoint come from the
server's `AWS_REGION` and `AWS_ENDPOINT_URL_S3` environment; there is no `seedConfig` option.

Cypher version. `existingData: 'use'` is required in Cypher 5 and deprecated in
Cypher 25, so the tool ties it to the language version: with `SEED_CYPHER_VERSION` unset
(Cypher 25) it is left out, as in the examples below; `SEED_CYPHER_VERSION=5` emits
`CYPHER 5 CREATE DATABASE … OPTIONS { seedURI: …, existingData: 'use' }`. Set it to match
your cluster's Cypher version.

### 1. Restore from a full

The latest artifact is a full (e.g. right after a full backup, no diffs yet). Recovers the
database as of that full.

```cypher
CREATE DATABASE `orders-20260629t120000`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<full>.backup' } WAIT
```

### 2. Restore from a differential chain (HEAD)

The latest artifact is a differential. Point `seedURI` at it; Neo4j finds the full and every
differential up to it in the same folder and recovers to the newest backed-up state.

```cypher
CREATE DATABASE `orders-20260629t120500`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup' } WAIT
```

### 3. Point-in-time recovery (PITR)

Add `seedRestoreUntil` to replay the chain up to a timestamp (or transaction id). Requires
a differential chain; a lone full fails with *"can only be fully restored."*

```cypher
CREATE DATABASE `orders-20260629t120900`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup',
            seedRestoreUntil: datetime('2026-06-29T11:00:00Z') } WAIT
```

After any of these: bring the new database online, optionally verify it, then cut over:
`ALTER ALIAS \`orders\` SET DATABASE TARGET \`orders-<ts>\``. Roll back by repointing the
alias to the previous physical; drop the old one after a soak.

Pluggable cutover. The alias swap is the default strategy. Teams that route apps through
an external layer (proxy / service discovery / DNS / router) set `CUTOVER_STRATEGY=external`
and `CUTOVER_HOOK=<url|command>`; the orchestrator calls the hook with the new/old physical so
that layer repoints, and the Neo4j alias is left untouched. Rollback is then the router's
concern (repoint back to the old physical). Default `CUTOVER_STRATEGY=alias-swap` is unchanged.

## By-name restore (no alias)

Teams that don't use aliases set a group's `restore_mode: by-name` (POLICY.md) and list
`databases:` (names, not aliases). Restore then targets each database by its own name:

- Create-if-absent (the default, changes nothing that exists), for a fresh cluster or a dropped database:
  `CREATE DATABASE <name> OPTIONS { seedURI: … }`. Nothing is dropped.
- Replace (opt-in, destroys the current database): an existing database is replaced with `DROP DATABASE
  <name>` then `CREATE … seedURI`. Requires `replace=true` on the restore (Dagster op config /
  Airflow `replace` param). There is a window where the database is gone; the Dagster op
  pre-validates every artifact + target across the group before any drop, and you should run
  `verify` first for restorability assurance.

There is no in-place restore that keeps a named database; Neo4j has no
`RENAME DATABASE` ([Ops Manual](https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/alter-databases/)),
so swapping a freshly-restored copy into an existing name isn't possible without an alias.
If you need non-destructive restore, use `alias-swap` (that indirection is the whole point).

## Seed from a local file (`file://`)

`seedURI` also accepts a file on the server: `CREATE DATABASE foo OPTIONS { seedURI:
'file:/seed/foo.backup' }`, with an absolute path, once `FileSeedProvider` is in the server's
`dbms.databases.seed_from_uri_providers`. A `.backup` file is the same whichever cloud it came
from, so this restores from a mounted copy of a backup, and the tests use it to check restores
from any store: download the file to the server, then seed from `file://`
(`just file-restore-smoke`). `seedRestoreUntil` works on `file://` too.

Cluster topology. When the group declares a `topology:` (POLICY.md), the orchestrator
adds it to the seed so the restored database comes up with the intended number of copies
rather than the DBMS default, which after losing servers can be fewer primaries than you had.
The three commands above gain a clause before `OPTIONS`:

```cypher
CREATE DATABASE `orders-20260629t120000`
  TOPOLOGY 3 PRIMARIES 0 SECONDARIES
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<full>.backup' } WAIT
```

Leave `topology:` out on a single server, where the clause is an error. Changing the value
and restoring gives the restored database the new shape (for example 3 primaries to 5).

## Recovery modes at a glance

| Mode | `seedURI` points at | Extra option | Recovers to | Needs a chain? |
|---|---|---|---|---|
| Full | a full `.backup` | — | that full's point | no |
| Differential (HEAD) | a differential `.backup` | — | newest backed-up state | resolves the chain |
| PITR | a differential `.backup` | `seedRestoreUntil: datetime(T)` | the chain replayed to T | yes |

## In the pipeline

`neo4j-backup restore <group>` (and the Dagster job and Airflow DAG) does this for every member
of a group:

- Without `--until`, the group goes back to the newest instant every member covers: the member
  whose newest backup is oldest is restored whole, the others to that time. The dry-run shows
  the point under `point`, or says why the members can't be aligned.
- With `--until T`, each member is seeded from the chain whose window covers T (an older
  database's chain if the alias has moved since), with `seedRestoreUntil` T.
- Every new database must be online on every server (within the group's `rto_minutes`) before
  any alias moves; privileges are copied, then the aliases move one by one, each recorded for
  `rollback`.

Locally:

```bash
just restore demo                          # the shell script: newest backup of each alias
just restore demo "2026-06-29T11:00:00Z"   # to a point in time
just demo-pitr                             # builds a full, change, diff chain and restores into it
```

## Users, roles, privileges and aliases

The restores above bring back database data. Users, roles, privileges and alias definitions live
in the `system` database, which a seed can't create, so they are backed up separately, in one of
two ways. Both run on the policy's `dbms:` crons.

| | Metadata export (default) | Binary `system` backup |
|---|---|---|
| What it is | the metadata as Cypher statements | `neo4j-admin` backup of the `system` database |
| Restored with | Cypher over Bolt, cluster running | `neo4j-admin` on the servers, Neo4j stopped |
| Native passwords | not kept: users get a placeholder and must change it | kept exactly |
| SSO / LDAP users | fully restored (no local secret) | not needed |
| Runs as | a job or DAG | a runbook (`bootstrap/restore_system.sh`) |

If your users sign in with SSO or LDAP, the export is enough. If they have native passwords you
must not reset, also take the binary backup.

### Metadata export

Dagster's `metadata_export` job or Airflow's `neo4j_dbms_metadata_<cluster>` DAG writes
`_dbms/metadata-<ts>.cypher` in the cluster's store. `metadata_restore` (Dagster),
`neo4j_metadata_restore` (Airflow) or `neo4j-backup metadata restore [--key <k>]` replays the
newest one (or the one named): `CREATE ROLE`/`CREATE USER … IF NOT EXISTS`, the role grants, the
statements from `SHOW PRIVILEGES AS COMMANDS`, and `CREATE ALIAS … IF NOT EXISTS`. Replaying
only adds what is missing, so it is safe to run twice.

What it can't bring back, because Neo4j doesn't return it:

- Native passwords. Users come back with a random password and `CHANGE REQUIRED`; reset them.
- Remote alias credentials. Those aliases are written with a `<<SUPPLY>>` placeholder and
  skipped on replay; recreate them.
- The database each alias points at is as of the export. After a full recovery the data
  restores move the user-database aliases themselves.

### Binary `system` backup

Dagster's `system_backup` job or Airflow's `neo4j_dbms_system_<cluster>` DAG runs
`neo4j-admin database backup system` (always a full) into `_dbms/system/`, keeping the newest
`dbms.keep_system` (default 14).

Restoring it needs Neo4j stopped, because `system` can't be seeded or stopped on its own:

1. Stop Neo4j on the server.
2. Restore from the bucket: `neo4j-admin database restore system --from-path=s3://<bucket>/<prefix>/_dbms/system/<newest>.backup --overwrite-destination=true`.
3. Start Neo4j.
4. Run the `restore_metadata.cypher` that the restore wrote (the database access privileges)
   against `system`.

`bootstrap/restore_system.sh` (`just restore-system`) does these steps on the demo's single
server. On a cluster, restore one server this way and let the others copy from it (or restore
each); adapt the script to your hosts.

Both are tested: `smoke_metadata` round-trips the export through both orchestrators, and
`bootstrap/restore_system.sh` brings back a dropped native user whose old password then works.

### Order for a full recovery

1. Rebuild the cluster.
2. Restore users, roles and privileges (replay the export, or restore the binary backup).
3. Restore each group's databases.

With the new cluster in the policy's `clusters:`, `neo4j-backup restore --from-cluster <lost>
--to-cluster <new>` does steps 2 and 3 in one plan (see
[OPERATOR.md](OPERATOR.md#recover-after-losing-a-cluster-or-move-a-group)).

## Caveats

- PITR needs differentials. A point in time is reachable only if a chain has diffs covering
  it, so the tier's `diff_cron` decides how fine-grained restores can be.
- Aggregation removes point-in-time restore inside the chain it collapses. `aggregate_cron`
  has no default for that reason.
- Seeds over http or ftp, and restores of a whole server, use `neo4j-admin database restore
  --restore-until` on the server itself instead of a seed.
