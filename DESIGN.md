# Design notes and history

These notes record design decisions from 2026-06 on: the context, the choice, the reasons and later changes.
Current behavior is on the reference pages: [POLICY.md](POLICY.md), [OPERATOR.md](OPERATOR.md),
[RECOVERY.md](RECOVERY.md), [ENVIRONMENT.md](ENVIRONMENT.md) and the [Dagster](orchestrator/README.md)
and [Airflow](airflow/README.md) pages. Terms: [GLOSSARY.md](GLOSSARY.md).
Diagrams are Graphviz sources in [`diagrams/`](diagrams/README.md). Render them with `just diagrams`.

## Starting assumptions

The target was self-hosted, clustered Neo4j Enterprise with 50 to 500 customers and up to a few thousand databases.
Teams already run Dagster or Airflow and object storage. A policy drives schedules and retention.

## The database group is the policy unit

A customer can own databases that reference each other and must come back to one point.
So the `db_group` carries cadence, retention, RPO/RTO and point-in-time alignment.
A database is the unit of work, since `neo4j-admin` acts on one database. The customer is an optional tag.
Group membership comes from the policy alone.

## Agentless: one central runner

One central runner runs `neo4j-admin database backup` against the backup port (6362).
It opens a Bolt connection (7687) for restore Cypher. Database nodes run only Neo4j.
Neo4j offers online backup through `neo4j-admin` alone and recommends a separate backup machine.
Back up from secondaries (`--from` takes a failover list), and add a non-serving one at hundreds of backups per tick.

## Restore with Cypher seed-from-URI

Restore uses `CREATE DATABASE ... OPTIONS { seedURI: ... }`, and every cluster server pulls the seed itself.
This keeps the loop agentless. The seed can be a full or a diff, and Neo4j resolves the chain.
Node-local `neo4j-admin database restore` stays a fallback for `system` and for seeds outside the server's read access.

```
CREATE DATABASE foo OPTIONS {
  seedURI: 's3://neo4j-backups/<group>/<db>/<artifact>.backup',
  seedRestoreUntil: datetime('2026-06-28T18:40:32.142+0100')
}
```

Validation found:

- `seedRestoreUntil` (Neo4j 2025.01+) needs a full plus diffs covering the time. A lone full errors.
- The cloud seed provider takes region and endpoint from the server's env, outside `seedConfig`.
- `existingData: 'use'` is required in Cypher 5 and deprecated in Cypher 25.
  `SEED_CYPHER_VERSION=5` emits it, and the default omits it.

## Alias-swap cutover and naming

Neo4j seeds only new databases, and an in-place restore takes the database down.
So applications connect to an alias, and a restore seeds a new database `<slug>-<ts>` next to the live one.
Once it is online on every member, `ALTER ALIAS` moves the alias. Rollback moves it back.
`cleanup` drops the superseded database later.
One `seedRestoreUntil` for every member lands a group on one wall-clock instant.

Naming has one owner (`bootstrap/naming.sh`, mirrored by `naming.py`):

| Identifier | Rule |
|---|---|
| Alias | The app-facing name, checked against Neo4j's alias rules and fixed for good |
| Slug | A path-safe id from the alias, with an 8-character hash for messy aliases |
| Physical | `<slug>-<ts>`, always a legal database name |

`ALTER ALIAS` aborts in-flight transactions on the alias, so applications must retry.
Later, `restore_mode: by-name` (#48) added restores under the database's own name.
Names became configurable (#88 to #91) through policy `naming:`, per-member overrides and `NAMING`.
Checks before the swap became opt-in (`restore --check`).

## Storage layout: one folder per physical store

A backup chain belongs to one physical store, and a restore starts a new store.
So the layout is `<group>/<slug>/<physical>/<artifact>.backup`, one chain per `<physical>/` folder.
The latest backup for an alias is the newest artifact across its `<slug>/` folder.
Load artifacts (#156) live in `<group>/<slug>/_seed/`, and the chain-head lookup skips them.
Since #118, each backup and aggregate writes `neo4j-admin backup inspect` output to `<artifact>.meta.json`.
Chains and restore windows are read from these sidecars.

## Runner resources: scratch and memory

`neo4j-admin` stages each backup on local scratch before upload, and some databases are several TB.
Scratch is its own sized volume (`SCRATCH_PATH`), separate from the orchestrator's disk.
A full lane runs with low concurrency (scratch size / largest full) and a diff lane with more.
Periodic `aggregate` keeps chains short.
By default the backup takes the server's page cache size and can OOM the runner, so set `--pagecache`.

| Resource | Knob | Start |
|---|---|---|
| JVM heap | `HEAP_SIZE` (`RUNNER_HEAP_SIZE`) | 2G |
| Page cache | `--pagecache` (`RUNNER_PAGECACHE`) | 512M to 1G |
| Scratch for a full | `--temp-path` | about the compressed database size |
| Consistency check | `--max-off-heap-memory` | cap it (the default is 90% of free RAM) |

Transaction-log retention (default `2 days 2G`) gates diffs. A diff whose logs are gone becomes a full.

## Verification: aggregate, then check

`neo4j-admin database check` refuses diffs and unrecovered fulls, and it is too heavy for a serving node.
So `neo4j-admin backup aggregate` builds a recovered full on the runner, and `database check` reads it.
Aggregating in place gives up point-in-time restore inside the chain.
So `verify` aggregates a copy under `_verify/`, and `aggregate` runs on its own schedule.

## Encryption at rest: SSE-KMS

`neo4j-admin` writes artifacts unencrypted. Client-side encryption is stronger,
but seeding needs objects the server can read. So the choice is SSE-KMS at the object store.
A per-group key means a bucket or key per group, and database nodes need KMS decrypt on those keys.
`neo4j-admin` omits the SSE header on upload, so a bucket that requires one rejects its uploads.
`BACKUP_UPLOAD=pipeline` keeps neo4j-admin on local disk while boto3 does every S3 write with `S3_SSE`.
Per-group `encryption` policy keys remain a design plan.

## One core, several front-ends

The operations live once in `neo4j_backup_core.ops` and take a `run_admin` callable.
Each front-end is a thin adapter. Dagster and Airflow add lanes and run history.
The `neo4j-backup` CLI is for CI and cron ([CLI-CONTRACT.md](CLI-CONTRACT.md), [CI.md](CI.md)).
The MCP server is for agents ([MCP.md](MCP.md)). Agents on the plain CLI follow [AGENTS.md](AGENTS.md).
On Dagster the tool is its own code location, and `neo4j-admin` runs through Pipes to report the real exit code.

## Bolt access and retries

All Bolt access goes through `Neo4jClient`. Only known-transient errors retry, with bounded backoff (`NEO4J_RETRY_*`).
Each attempt rebuilds the driver to pick up rotated secrets.
Errors are classified by exception type and status code, ignoring message text.
Waits are bounded polls or `WAIT`.

## Configurable seams

Each deployment difference is a small interface with a default, picked by env or policy.
Examples are `SECRET_PROVIDER`, `CUTOVER_STRATEGY`, `PATH_LAYOUT`, `POLICY_LOADER` and `CLOUD` (#52).
Third-party SDKs import lazily. The full list is in [ENVIRONMENT.md](ENVIRONMENT.md).

## Many clusters from one policy

One DBMS becomes memory- and checkpoint-bound well before a thousand databases.
So `clusters:` names each DBMS (#106), and a group says `cluster:`.
Every operation resolves its client, runner and store from the group, which also spreads backup load.
Group ids are unique, so storage paths omit the cluster.
DBMS jobs write under `_dbms/` for the default cluster and `_dbms/<cluster>/` for a named one.

## Other choices

Neo4j Ops Manager (checked through 1.15.2 in 2026-05) lacked a released backup feature and a public API.
It also puts an agent on every node, so it fits only as a monitoring tool.

`system` holds users, roles, privileges and aliases, and seed-from-URI covers user databases only.
The default path is a logical export as Cypher, which drops native passwords.
A binary `system` backup with an offline node-local restore keeps them. See [RECOVERY.md](RECOVERY.md).

## Status and open risks

Validated features: [STACK.md](STACK.md#validation-status). Open risks:

- Point-in-time restore needs a full plus diffs. A lone full restores whole only.
- A group's databases line up within bounded clock skew, short of transactional consistency.
- Pin the orchestrator host and the code location or worker to close versions.
- SSE-KMS reads through seed-from-URI were validated on MinIO. AWS S3 with KMS is still pending.
