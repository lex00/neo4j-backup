# Local Dev Stack

Single-node Neo4j Enterprise + MinIO object store + a node-agentless backup runner,
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
just restore demo # restore the group via seed-from-URI into <db>_restored
just urls         # Neo4j Browser + MinIO console links
```

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
| `restore [group] [until]` | seed-from-URI restore (optional PITR) into `<db>_restored` |
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

- `acme_orders` — customers and orders
- `acme_graph` — products
- `acme_audit` — append-only event log

The PITR alignment story uses this group: one `seedRestoreUntil` timestamp across all
three so they restore to a single consistent instant.

## Validation status (2026-06-29)

Validated end-to-end on this stack (`just fresh` → `backup` → `restore`):

- **MinIO plain-HTTP seed + path-style** — works; no TLS proxy or `MINIO_DOMAIN` needed.
- **SSE-KMS at rest + read via seed-from-URI** — works; `mc stat` shows
  `Encryption: SSE-KMS (arn:aws:kms:demo-group-key)`, and the server reads it
  transparently on restore.
- **`eval` license** — Neo4j Enterprise booted fine.
- **Backup → restore → alias swap → data verified** through alias routing.

Two fixes applied during validation (now in the scripts): `latest_artifact` uses
`mc find` + host parsing (the `mc` image has no awk/grep); and the restore drops
`seedConfig` (CloudSeedProvider rejects it — region/endpoint come from server env).

Remaining notes:

- **PITR demonstrated** via `just demo-pitr`: builds a full → change → differential
  chain, then `seedRestoreUntil=T0` restores the pre-change state (2 customers) while
  HEAD restores 3. Confirms `seedRestoreUntil` needs a differential chain (a lone full
  errors with "can only be fully restored").

## Parked: cluster topology (for later)

When topologies earn their keep, add `docker/compose.cluster.yaml` based on the
verified current-version config below (3 primaries + 1 secondary). Key points:

- Image tag `>= 2025.01` (current discovery model).
- Discovery is unified under `dbms.cluster.endpoints` (NOT the old
  `dbms.cluster.discovery.v2.endpoints`); the official tutorial uses a shared network
  alias with `dbms.cluster.discovery.resolver_type=DNS` and
  `dbms.cluster.endpoints=neo4j-network:6000`.
- Per-server env: `NEO4J_initial_server_mode__constraint=PRIMARY` (or `SECONDARY`).
- `dbms.cluster.minimum_initial_system_primaries_count` defaults to 3 (set to 2 for a
  two-server cluster).
- Default ports: cluster/discovery 6000, Raft 7000, routing 7688, backup 6362.
- Back up from a **secondary/follower**: set `NEO4J_BACKUP_SOURCE` to that member's
  `:6362`. The resolution order already prefers secondary → follower → writer.

Source: Neo4j "Deploying a Neo4j cluster in a Docker container" tutorial and the
clustering discovery / ports references (current docs).
