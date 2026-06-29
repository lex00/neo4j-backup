# Neo4j Backup & Restore Solutions: Landscape, Feature Matrix, and Gaps

Research compiled 2026-06-28. Based on Neo4j primary documentation, vendor primary
docs (GrapheneDB, Commvault), and open-source project repositories. Each core claim
was adversarially verified by a 3-vote panel (23 of 25 claims survived; 2 refuted).

See [`findings.md`](findings.md) for the verified claim ledger, refuted claims,
open questions, and full source list.

## The three tiers at a glance

Neo4j backup capability splits hard along edition and hosting lines:

- **Native tooling** is the richest but gates online/incremental/PITR behind
  **Enterprise Edition**. Community Edition gets offline, full-only `dump`/`load`.
- **Aura** (managed cloud) reimplements backup as point-in-time snapshots with
  tier-scaled cadence and retention.
- **Commercial third parties** (GrapheneDB, Commvault) wrap or replicate the native
  model and inherit its Enterprise requirement for hot backup.
- **Open source** fills the Community Edition hole with logical/export-based tools,
  none of which offer true incremental or PITR.

## Solutions inventoried

### Native (Neo4j-supplied)
- `neo4j-admin database backup` / `restore` — online (hot), full + differential,
  PITR, backup chains. Enterprise only, not on Aura.
- `neo4j-admin database dump` / `load` — offline, full-only. All editions including
  Community.
- `neo4j-admin aggregate` — consolidates a backup chain into one full artifact.
  Enterprise only.

### Managed
- Neo4j Aura — snapshot-based, tier-scaled (Free / Professional / Business Critical /
  Virtual Dedicated Cloud).

### Commercial third-party
- GrapheneDB — managed Neo4j DBaaS on AWS; daily + on-demand online snapshots,
  consistency-checked, ~1-week retention.
- Commvault — enterprise backup product; full + incremental, requires Neo4j
  Enterprise for online mode.

### Open source (logical/export-based, Community-friendly)
- APOC export (`apoc.export.*`) — Cypher/JSON/CSV/GraphML/Gephi/Excel.
- `andreshyer/neo4j-backup` — Python, node-by-node to gzip JSON, no dump/APOC needed.
- `Akagitsunee/neo4j-community-export-tool` — Docker + cron, exports Cypher scripts
  while DB is live.
- `jexp/neo4j-shell-tools` — import/export utility (CSV, GraphML, Geoff, binary);
  not a backup product.
- Ansible + `apoc.export.graphml.all` — orchestration pattern over APOC.

## Feature category matrix

Legend: yes / partial (qualified) / no / "?" = not confirmed by sources (see Open
Questions in findings.md — absence means unconfirmed, not necessarily unavailable).

### Backup mechanics

| Feature | Native online (EE) | Native dump (all eds) | Aura | GrapheneDB | Commvault | APOC / OSS export |
|---|---|---|---|---|---|---|
| Online / hot backup | yes | no (offline) | yes (snapshots) | yes (no downtime) | yes (needs EE) | partial: live export, not txn-consistent |
| Offline backup | no | yes | n/a | no | no | partial: can run offline |
| Full backup | yes | yes | yes | yes | yes | yes |
| Incremental / differential | yes (delta of tx logs) | no | partial: diff snapshots exist but not restorable/exportable | no (daily fulls) | yes | no |
| Backup chains | yes (full + n diffs) | no | internal | ? | partial | no |
| Chain aggregation | yes (`aggregate`) | no | n/a | ? | ? | no |

### Recovery

| Feature | Native online (EE) | Native dump | Aura | GrapheneDB | Commvault | OSS export |
|---|---|---|---|---|---|---|
| Point-in-time recovery | yes `--restore-until` (txID or timestamp, within a chain) | no | yes via snapshots (discrete points, not continuous) | partial: restore to prior snapshot | partial: inherits native model | no |
| Restore granularity | whole database | whole database | whole instance | whole database | whole database | partial: selective (nodes/rels/queries via APOC) |
| Restore target options | same/new DB | same/new DB | overwrite in place or new instance | restore to instance | NFS/objectstore restore | import into any instance |
| Cross-version restore | partial: same or later only; downgrade unsupported | partial: load same/later | partial: export needs self-managed 5.20+; v5→v4 unsupported | ? | ? | yes: format-portable (logical) |
| Consistency check | yes | partial: available | internal | yes (before each backup) | ? | no |

### Operations, scheduling, retention

| Feature | Native online (EE) | Native dump | Aura | GrapheneDB | Commvault | OSS export |
|---|---|---|---|---|---|---|
| Scheduling / automation | yes: independent full/diff cadences (2026.02+) | external cron | yes: tier-fixed cadence | yes: daily auto + on-demand | yes: scheduled | partial: cron / Ansible (DIY) |
| Retention policy | no auto-pruning (operator-managed) | DIY | tier-fixed (7 / 30 / 60-90 days) | ~1 week | configurable | DIY |
| Compression | yes: `--compress`, default true | supported | internal | tarball | ? | partial: gzip (neo4j-backup) |
| Monitoring / alerting | no native metrics/alerting; exit codes + logs + `--keep-failed` | exit codes | no failure alerting documented | ? | yes (product feature) | DIY |
| Edition / licensing required | Enterprise | Community+ | Aura subscription | GrapheneDB plan | Neo4j EE for online | Community+ (free) |

### Cloud targets, encryption, access control (second-pass, verified)

| Feature | Native online (EE) | Native dump | Aura | GrapheneDB | Commvault | OSS export |
|---|---|---|---|---|---|---|
| Direct cloud storage target | yes: `s3://` / `gs://` / `azb://` (+ S3-compat: Ceph/Minio/LocalStack) | yes: same URI schemes | n/a (managed); export download | tarball download | yes: NFS/objectstore | DIY (local files) |
| Encryption in transit | yes: `backup` SSL scope (port 6362, mutual auth option) | n/a (offline) | yes: TLS always required | ? | yes | depends on driver TLS |
| Encryption at rest (artifact) | not built-in (storage/FS layer) | not built-in | yes: 256-bit AES (cloud SSE) + optional CMK | ? | yes (product) | no |
| RBAC on backup ops | OS-level only (run as `neo4j` user; no Cypher privilege) | OS-level | console RBAC: PROJECT_ADMIN only | account roles | granular RBAC (explicit backup/restore perms) | OS-level |
| Cluster-aware backup | yes: any member; secondary→follower→writer resolution; `--from` failover list | n/a | managed | managed | via native | no |

### Aura tier breakdown (verified)

| Tier | Scheduled cadence | Retention | On-demand |
|---|---|---|---|
| Free | none | n/a | yes (one at a time, no rolling backups) |
| Professional | daily | 7 days | yes |
| Business Critical | hourly (qualified, see caveats) | 30 days | yes |
| Virtual Dedicated Cloud | daily full | 60 days restorable / up to 90 exportable | yes |

Aura restore: overwrite in place or spawn a new instance, on demand via console or
Aura API. Exported snapshots import into Community Edition (self-managed needs 5.20+).

## Gaps

### Cross-tier capability gaps (verified)

1. **The Community cliff.** Online/hot backup, incremental/differential, PITR, and
   chain aggregation are uniformly Enterprise-or-managed. Community Edition has only
   offline, full-only `dump`/`load`. There is no native hot-backup, incremental, or
   PITR path for Community. The OSS export tools exist specifically to paper over
   this, trading away transactional consistency, incremental capture, and PITR.

2. **Commercial tools don't escape the edition gate.** Commvault's online mode still
   requires Neo4j Enterprise; it replicates the native full+incremental model rather
   than adding a Community hot-backup path. GrapheneDB only avoids this because it
   runs Enterprise for you.

3. **No downgrade restore in the native/managed path.** Restore works to the same or
   later Neo4j version only; v5→v4 and general downgrades are unsupported. Only the
   logical OSS exporters are format-portable across versions.

4. **Aura differential snapshots are second-class.** They capture changes since the
   last full but cannot be restored, exported, or used to seed a new instance. Only
   full snapshots are actionable.

5. **OSS has no real backup product.** Every open-source option is a logical
   export/import utility (APOC, neo4j-backup, neo4j-shell-tools, Ansible patterns),
   not a backup system. None offer incremental, PITR, consistency checks, retention
   management, or cluster-aware backup.

### Real gaps after the second-pass docs review

The second pass (see [`second-pass-neo4j-docs.md`](second-pass-neo4j-docs.md)) closed
most first-pass open questions. What remains a genuine gap:

6. **No native at-rest encryption of self-managed artifacts.** Online/dump artifacts
   are not encrypted by neo4j-admin; protection is delegated to the filesystem or
   storage layer. Only Aura encrypts at rest (256-bit AES, optional CMK).

7. **No native backup monitoring or alerting.** Self-managed backup reports outcome
   only through exit codes and logs (`--keep-failed` preserves a failed run). There is
   no backup metric in the Metrics Reference and no built-in failure alerting. Aura
   likewise documents no backup-failure notification.

8. **No native retention automation.** Neither self-managed nor Aura auto-prunes
   backup artifacts/chains on an operator-defined policy. Self-managed retention is
   fully DIY; Aura retention is tier-fixed with no operator control. Note the
   transaction-log retention default (`db.tx_log.rotation.retention_policy = 2 days
   2G`) silently forces a full backup if logs needed for a differential have rotated.

9. **No cluster-wide transactional-consistency guarantee.** Cluster backups are a
   single-member operation (full backups force a checkpoint on that member); the docs
   do not claim a cluster-wide consistent snapshot.

### Resolved by the second pass (previously listed as gaps)

- Encryption in transit: confirmed (`backup` SSL scope, port 6362; Aura TLS always on).
- Compression: confirmed (`--compress`, default true).
- Cloud storage targets: confirmed — native backup AND dump write directly to
  `s3://`, `gs://`, `azb://`, plus S3-compatible endpoints (Ceph/Minio/LocalStack).
- RBAC: self-managed is OS-level only (no Cypher privilege); Aura gates snapshots to
  PROJECT_ADMIN; GrapheneDB uses account roles; Commvault has granular backup RBAC.
- Cluster awareness: confirmed — any-member backup, secondary-preferred resolution,
  `--from` failover list, seed-from-URI / designated-seeder restore with topology
  redefinition.

## Confidence and caveats

23 of 25 verified claims survived (2 killed). Two claims were refuted:
- "Aura provides automated backups across all tiers" — false; Free is on-demand only
  (0-3 vote).
- A specific higher-tier cadence breakdown (Business Critical hourly / VDC daily +
  hourly) — refuted 1-2.

The Aura tier numbers are version- and date-sensitive: "classic" vs "current" Aura
docs differ on VDC (60 restorable vs 60/90 split), and the per-tier hourly/daily
semantics are the softest part of this report. Commvault evidence is from the 2024e
release only. The 2026.02 independent full/diff scheduling feature is recent and
applies to current Neo4j versions.
