# Local Dev Stack

Single-node Neo4j Enterprise + the floci S3/KMS emulator + a node-agentless backup runner,
driven by `just`. Single node is deliberately the starting point: it exercises the
whole backup/restore loop (online backup, seed-from-URI restore, PITR). Cluster
topologies are deferred until source-member selection and placement actually need
testing; the verified config is parked at the bottom of this file for that day.

## Prerequisites

- Docker + Docker Compose
- `just`
- Neo4j Enterprise eval license is accepted in-container via
  `NEO4J_ACCEPT_LICENSE_AGREEMENT=eval` (dev/eval use).

## Quick start

```
just fresh        # up + bootstrap: boots the stack and the demo group from scratch
just backup demo  # online backup of the demo group to object storage
just artifacts    # list what landed in the bucket
just restore demo # seed a new database per alias from its newest backup, then swap the aliases
just urls         # Neo4j Browser link and the local S3 endpoint
```

The same loop runs through each front-end against this stack: `just cli-smoke` drives the
`neo4j-backup` CLI end to end (backup → verify → restore → prune), `just airflow-smoke` the Airflow
DAGs, and `just mcp-smoke` a read-only round trip against the operator [MCP server](MCP.md) (needs
the `[mcp]` extra). See [CLI-CONTRACT.md](CLI-CONTRACT.md) / [CI.md](CI.md) / [AGENTS.md](AGENTS.md).

Point-in-time restore (group-aligned to one instant):

```
just restore demo "2026-06-28T12:00:00Z"
```

## What `just` gives you

| recipe | does |
|---|---|
| `up` / `down` | start / tear down the stack (`down` removes volumes) |
| `bootstrap` | create the demo group's databases, load demo data |
| `fresh` | `up` + `bootstrap` |
| `backup [group]` | `neo4j-admin database backup` of each db in the group to `s3://…` |
| `restore [group] [until]` | seed a new database per alias from its newest backup (or to `until`), then swap the aliases |
| `artifacts [prefix]` | list backup artifacts in object storage |
| `logs` / `ps` / `urls` | observe the stack |

## Component placement

Two networks model the real boundary:

- `neo4j` — database members only.
- `ops` — object storage (and, later, the orchestrator).

The **runner** is the only component bridging both for *backups*: it pulls over the
backup port (6362) and writes to object storage. Nothing runs on the database node for
backup. Note one consequence of seed-from-URI restore: the **Neo4j server itself**
pulls the seed from object storage, so the DB node is also on `ops` and carries S3
credentials. In a real cluster every member needs that object-store egress to restore.

## Demo group

`policies/demo.yaml` defines group `demo` (tier gold) over three databases that
reference each other logically (shared customer/product ids):

- `acme-orders`: customers and orders
- `acme-graph`: products
- `acme-audit`: an append-only event log

The PITR alignment story uses this group: one `seedRestoreUntil` timestamp across all
three so they restore to a single consistent instant.

## Object store: floci

The local object store is [floci](https://hub.docker.com/r/floci/floci) (`floci/floci:1.5.34`), a
LocalStack-compatible AWS emulator, with the AWS CLI (`amazon/aws-cli`) for setup and listing. It
replaced MinIO in 2026-11, when MinIO's images stopped being publicly pullable. Inside the
compose network the endpoint is `http://s3:4566`; on the host it is `http://localhost:9000`. It
accepts any credentials.

`createbuckets` (`docker/create-bucket.sh`) creates the KMS key `alias/demo-group-key` and the
`neo4j-backups` bucket, and sets the bucket's default encryption to that key. floci records the
default but does not apply it to objects, so neo4j-admin's direct writes are stored unencrypted
locally. Writes that send the SSE-KMS header themselves are encrypted: run with
`BACKUP_UPLOAD=pipeline` and `S3_SSE=aws:kms` to see it. On real S3 the bucket default applies to
every write, as before.

## Validation status (2026-11-15, floci)

Validated end-to-end on this stack: `just fresh`, `just backup demo`, `just artifacts`,
`just restore demo` (seed-from-URI over plain HTTP, path-style, then alias swap), `just demo-pitr`,
`just cli-smoke`, `just import-smoke`, `orchestrator/smoke_local.py` (Dagster) and
`just restore-system`.

Earlier notes (MinIO, 2026-06-29): seed-from-URI worked over plain HTTP with path-style
addressing, SSE-KMS objects were read transparently on restore, and the restore drops
`seedConfig` (CloudSeedProvider rejects it; region and endpoint come from the server's
environment).

Remaining notes:

- **PITR demonstrated** via `just demo-pitr`: builds a full → change → differential
  chain, then `seedRestoreUntil=T0` restores the pre-change state (2 customers) while
  HEAD restores 3. Confirms `seedRestoreUntil` needs a differential chain (a lone full
  errors with "can only be fully restored").

## A three-server cluster

`docker/compose.cluster.yaml` adds three Neo4j servers (`core1`-`core3`) beside the base
stack:

```bash
docker compose --env-file .env -f docker/compose.yaml -f docker/compose.cluster.yaml \
  --profile cluster up -d --wait core1 core2 core3
just cluster-smoke
```

Host clients use `neo4j://localhost:7691` (7691-7693 are the three servers' Bolt ports); the
runner reaches the backup ports as `core1:6362`, `core2:6362`, `core3:6362`. Discovery is
`dbms.cluster.endpoints`, each server advertises its own name, and the health check queries
`system` because the default database lives on only some of the servers. `just cluster-smoke`
backs up a database hosted as 1 primary and 2 secondaries, restores it with that topology and
checks every server sees the alias on the new database. `just down` removes these containers
too.

## A second cluster

`docker compose --env-file .env -f docker/compose.yaml --profile multi up -d --wait neo4j-b`
starts a second, independent Neo4j (Bolt on host port 7688, backup port `neo4j-b:6362` on the
stack network) that reads the same bucket. `just move-smoke` uses it as cluster `west` to test
`restore --to-cluster` and `--from-cluster`.
