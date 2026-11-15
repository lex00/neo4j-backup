# Operator guide

For the platform team that runs Neo4j for the rest of the organization and uses neo4j-backup to
back up and restore its customers' databases. Your customers (application teams, data teams) use
the databases; they may have credentials on the cluster but not on Dagster or Airflow, so when they
need a restore they ask you.

The idea is that you edit one policy file and it drives every job: what is backed up, how often,
and how long it is kept. Restores are started by you when a customer asks. Approvals stay with
your team's own process; the tool's guard is `--dry-run` to preview and `--confirm` to apply.

Each section below gives the steps that work today. Steps marked **Planned** link to the issue
that will add them; until then, do them by hand as described.

If you haven't run the tool yet, start with [Getting started](GETTING-STARTED.md).

## How it fits together

- The **policy** ([POLICY.md](POLICY.md)) lists groups of databases. A group is the unit that
  restores to one instant; usually one customer, or one customer's related databases. Each group
  has a tier (when full and differential backups run) and a retention.
- A **scheduler** runs the jobs: Dagster schedules, Airflow DAGs, or cron/CI calling the CLI.
- A **runner** with `neo4j-admin` pulls each backup over the backup port and writes it to the
  bucket under `<group>/<slug>/<database>/`, where the slug is the alias (or a cleaned-up form of
  it when the alias has characters a path can't use). Nothing runs on the database servers.
- A **restore** creates a new database from a backup (`CREATE DATABASE … seedURI`, pulled by the
  Neo4j servers from the bucket), then moves the customer's alias to it. The old database stays
  until you drop it.

Applications must connect through the alias. Anything that uses the database name directly breaks
at every restore, because the name changes.

## Onboard a customer

1. Pick a tier. The example policy has `gold` (daily full, hourly differential), `silver`
   (weekly full, differential every 6 hours) and `bronze` (weekly full, daily differential). The
   differential interval is how much a customer can lose in a restore.
2. Make sure the database is behind an alias. For a new database, create it and an alias
   (`CREATE DATABASE`, then `CREATE ALIAS <alias> FOR DATABASE <database>`). For an existing one,
   create an alias with a different name and move the application to it; reusing the same name
   needs a migration ([`bootstrap/adopt.sh`](bootstrap/adopt.sh) shows the steps).
3. Grant the customer's role on the database. Grants made through the alias are stored
   against the database behind it; see [the restore runbook](#restore-on-a-customers-request) for
   what that means after a restore.
4. Add a group to the policy with the alias, tier and `retention_days`, and deliver the policy
   (a file the workers read, or `s3://…` with `NEO4J_BACKUP_POLICY`). Dagster's
   `reconcile_registry` sensor adds the new targets within five minutes; Airflow picks them up at
   the next DAG parse.
5. If the tier is new, switch on its schedules: Dagster creates them stopped.
6. Run the first backup now rather than waiting for the schedule, then verify it:
   `neo4j-backup backup <group> --kind FULL` and `neo4j-backup verify <group>`.
7. Tell the customer their tier, how far back they can restore (`retention_days`), and how to
   ask for a restore. The [Customer guide](CUSTOMERS.md) is written for them.

**Planned:** customer types as reusable profiles
([#101](https://github.com/lex00/neo4j-backup/issues/101)), a `tenant` label to select all of a
customer's groups ([#102](https://github.com/lex00/neo4j-backup/issues/102)), `policy validate`
for pull requests on the policy file ([#101](https://github.com/lex00/neo4j-backup/issues/101)),
an `adopt` command ([#92](https://github.com/lex00/neo4j-backup/issues/92)), and an option for
new schedules to start running ([#104](https://github.com/lex00/neo4j-backup/issues/104)).

## Check that customers are covered

- Runs: failed backups show as failed runs in Dagster or Airflow, or as a non-zero exit from
  the CLI in CI or cron.
- Freshness: the MCP tool `backup_status` lists every target's newest backup and its age in
  hours ([MCP.md](MCP.md)).
- Restorability: `neo4j-backup verify <group>` copies the latest backups aside, combines them
  into a full backup and runs a consistency check, without touching the cluster. Airflow runs it
  daily (`neo4j_verify`); in Dagster, materialize the `verify` asset or schedule it yourself.

**Planned:** freshness checked against each group's `rpo_minutes`
([#123](https://github.com/lex00/neo4j-backup/issues/123)), a `restore-points` command
([#113](https://github.com/lex00/neo4j-backup/issues/113)), and verify/aggregate/prune schedules
set in the policy ([#104](https://github.com/lex00/neo4j-backup/issues/104)).

## Restore on a customer's request

### 1. Get the request straight

Ask the customer for the group or alias, the point in time in UTC (or "latest"), and your ticket
reference. Check that they understand a restore covers every database in the group: they all go
back to the same instant.

### 2. See what can be restored

List the group's backups in the bucket:

```bash
aws s3 ls --recursive s3://$BACKUP_BUCKET/<group>/
```

Or use the MCP tools `show_chain(group, member)` and `latest_artifact(group, member)`. A point in
time is reachable when a differential backup after it exists in the same database folder as a
full backup before it.

### 3. Tell the customer what will happen

- Writes after the chosen time are gone.
- Transactions running at the moment of the switch are aborted and rolled back; applications must
  retry. On a cluster, members may briefly disagree about where the alias points.
- Their users lose access until you copy the privileges (step 6).

### 4. Preview

```bash
neo4j-backup restore <group> --dry-run                                   # latest
neo4j-backup restore <group> --until 2026-11-15T09:00:00Z --dry-run      # point in time
```

The plan lists the backup each database will come from and any database that would be dropped
(only in by-name groups with `--replace`).

### 5. Restore

```bash
neo4j-backup --json restore <group> --until 2026-11-15T09:00:00Z --confirm > restore.json
```

The same restore from the orchestrators:

- Dagster: launch the `restore_group` job with config
  `{ops: {restore_group_op: {config: {group_id: <group>, restore_until: "2026-11-15T09:00:00Z"}}}}`.
- Airflow: `airflow dags trigger neo4j_restore -c '{"group_id": "<group>", "restore_until": "2026-11-15T09:00:00Z"}'`.

The result lists, per member, the old database (`old`) and the new one (`newdb`). Keep it; you need
both names below.

### 6. Copy the customer's privileges to the new database

A known issue in this release ([#117](https://github.com/lex00/neo4j-backup/issues/117)): Neo4j
stores privileges against the database, not the alias, so after the switch the customer's users
get "Access denied". For each member, with `OLD` and `NEW` from the restore result:

```bash
cypher-shell -d system --format plain \
  "SHOW PRIVILEGES AS COMMANDS YIELD command WHERE command CONTAINS '\`$OLD\`' RETURN command" \
  | tail -n +2 | sed -e 's/^"//' -e 's/"$//' -e "s/\`$OLD\`/\`$NEW\`/g" \
  | while IFS= read -r cmd; do cypher-shell -d system "$cmd" </dev/null; done
```

This was tested on the local stack: a user granted read access through the alias was denied after
a restore and could read again after the copy. The privileges on the old database stay, which is
what you want for an undo.

### 7. Take a full backup of the restored group

```bash
neo4j-backup backup <group> --kind FULL
```

The new databases have no backups of their own yet, so the next scheduled differential backup
would fail ([#74](https://github.com/lex00/neo4j-backup/issues/74)) and anything written until the
next full backup would be unprotected.

### 8. Report back

Send the customer the restore point, the time of the switch, and the old and new database names.

### Undo a restore

The old database is still there. Point the alias back:

```cypher
ALTER ALIAS `<alias>` SET DATABASE TARGET `<old database>`
```

Its privileges were never removed, so access works again straight away.

### Clean up later

After a soak period, drop the old databases (`DROP DATABASE <old>`) and revoke the privileges
that were copied away from them.

### Limits today

- A restore always covers the whole group; one database alone isn't possible
  ([#121](https://github.com/lex00/neo4j-backup/issues/121)).
- `--until` uses the newest backup chain. A time before the last restore lives in the previous
  database's chain; restore that by hand with the Cypher in [RECOVERY.md](RECOVERY.md)
  ([#114](https://github.com/lex00/neo4j-backup/issues/114)).

**Planned:** copying privileges automatically ([#117](https://github.com/lex00/neo4j-backup/issues/117)),
the full backup as part of the restore ([#97](https://github.com/lex00/neo4j-backup/issues/97)),
`--ticket` and `--reason` on the command ([#119](https://github.com/lex00/neo4j-backup/issues/119)),
`rollback` ([#122](https://github.com/lex00/neo4j-backup/issues/122)) and `cleanup`
([#93](https://github.com/lex00/neo4j-backup/issues/93)).

## Load a customer's data in bulk

When a customer's data starts as CSV or Parquet files, follow [IMPORT.md](IMPORT.md): import it
on a separate loader machine, back it up into the group's folder in the bucket, then
`neo4j-backup restore <group> --confirm`. Copy privileges and take a full backup afterwards, as for
any restore.

For a customer whose graph is rebuilt in full from external sources on a schedule, run the same
steps each cycle. Their writes since the previous load are replaced each time.

**Planned:** a `load` command that runs the whole sequence
([#107](https://github.com/lex00/neo4j-backup/issues/107)), scheduled rebuilds from the policy
([#108](https://github.com/lex00/neo4j-backup/issues/108)), custom build steps such as RDF
conversion ([#109](https://github.com/lex00/neo4j-backup/issues/109)), checks before the switch
([#111](https://github.com/lex00/neo4j-backup/issues/111)), read-only databases between loads
([#112](https://github.com/lex00/neo4j-backup/issues/112)), and dump files as the load artifact
([#98](https://github.com/lex00/neo4j-backup/issues/98)).

## Change a customer's tier

Change the group's `tier` in the policy. The group moves to that tier's schedules. If you move to a
tier whose full backups are further apart than the group's `retention_days`, read
[#73](https://github.com/lex00/neo4j-backup/issues/73) first.

## Offboard a customer

Remove the group from the policy; its schedules stop. Its backups stay in the bucket and nothing
deletes them: remove `s3://<bucket>/<group>/` yourself once your retention obligations allow, and
drop the customer's databases and aliases on the cluster.

**Planned:** a `retired` state with a final retention period
([#124](https://github.com/lex00/neo4j-backup/issues/124)).

## Recover after losing a cluster

Not yet tested end to end. The pieces exist:

1. Bring up the replacement cluster and give its servers read access to the bucket.
2. Restore users, roles and privileges: `neo4j-backup metadata restore --confirm` replays the
   latest export (passwords aren't in it), or restore the `system` database offline for exact
   copies ([RECOVERY.md](RECOVERY.md#the-metadata-layer-users-roles-privileges-aliases)).
3. Point the tool at the new cluster (`NEO4J_BOLT_URI`, `NEO4J_BACKUP_SOURCE`) and restore each
   group. Check each alias and copy privileges as in the restore runbook.

**Planned:** several clusters in one policy ([#106](https://github.com/lex00/neo4j-backup/issues/106))
and restoring a group into another cluster ([#115](https://github.com/lex00/neo4j-backup/issues/115)).

## Notice changes customers make directly

Customers with cluster credentials can change things the tool depends on. The most important: if
an alias is moved to another database, backups follow the alias to a database with no full backup,
and differential backups fail there until the next full one. Check `SHOW ALIASES` against your last
restore results when backups for a group start failing.

## Settings

Everything below is optional and defaults to the tested behaviour. The full list is the
[environment table](orchestrator/README.md#environment-variables).

- Credentials: `SECRET_PROVIDER` reads the Neo4j password from AWS Secrets Manager (or your own
  provider) at each connection, so rotation needs no redeploy.
- Bolt retries: transient cluster errors (leader changes, dropped sessions, expired tokens)
  are retried a bounded number of times, chosen by Neo4j status code.
- Seed topology: set `topology` in the policy so a restore comes back with the intended number
  of primaries and secondaries instead of the cluster default.
- Cypher version: `SEED_CYPHER_VERSION=5` on a Cypher 5 cluster (adds the `existingData`
  option it requires).
- Cutover: the default moves the alias; `CUTOVER_STRATEGY=external` calls your hook to repoint
  a router or proxy instead.
- Storage layout: `PATH_LAYOUT` brings your own bucket key scheme.
- Policy source: `NEO4J_BACKUP_POLICY=s3://…` lets you change the policy without a redeploy,
  with a cache and last-known-good fallback (`POLICY_CACHE_TTL`); `POLICY_LOADER` for an
  authenticated source such as Vault.
- Encryption on every write: `S3_SSE` adds the SSE-KMS header to the tool's own writes, and
  `BACKUP_UPLOAD=pipeline` sends neo4j-admin's backups through the same path, for buckets that
  reject writes without the header.

## Commands

| Command | Does | Changes anything? |
|---|---|---|
| `targets` | lists the policy's groups and members | no |
| `backup <group> [--kind AUTO\|FULL\|DIFF]` | backs up every database in a group | adds backups |
| `verify <group>` | checks the latest backups restore and are consistent | no |
| `aggregate <group>` | combines each chain into one full backup, in place | yes |
| `restore <group> [--until <time>] [--replace]` | restores a group | yes |
| `prune` | deletes backups older than each group's retention | yes |
| `metadata export` / `metadata restore [--key <k>]` | users, roles, privileges and aliases as Cypher | restore changes the cluster |
| `system-backup` | full backup of the `system` database | adds a backup |
| `import <database> -- <args>` | bulk import on a loader ([IMPORT.md](IMPORT.md)) | yes |

Every command follows the [CLI contract](CLI-CONTRACT.md): `--json` prints one result object, exit
codes are stable for scripts, and commands that change things need `--confirm` (preview with
`--dry-run`). [AGENTS.md](AGENTS.md) covers driving the CLI from an agent; [MCP.md](MCP.md) the
operator MCP server.
