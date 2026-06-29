# Neo4j Backup Research: Verified Claim Ledger

Generated 2026-06-28 by deep-research workflow. 5 search angles, 19 sources fetched,
85 claims extracted, 25 verified by 3-vote adversarial panel, 23 confirmed.

## Confirmed claims (high confidence unless noted)

1. **Online vs offline / editions.** Neo4j Enterprise provides online/hot backup via
   `neo4j-admin database backup`; offline `dump`/`load` is available in all editions
   including Community but is full-only. Differential exists "solely within the
   context of online operations." (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/modes/
   - https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
   - https://neo4j.com/docs/operations-manual/current/backup-restore/planning/

2. **Full-then-differential model.** First backup into a target is always full;
   subsequent backups transfer only the delta of transaction logs, forming a backup
   chain (full anchor + n contiguous differentials). Falls back to full if required
   tx logs are unavailable. (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/modes/
   - https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/

3. **Restore + PITR.** Enterprise-only `neo4j-admin database restore` ingests full and
   differential artifacts, reconstructs the chain ending at a specified differential
   by replaying tx logs, and supports PITR via `--restore-until` accepting a
   transaction ID or timestamp. PITR is within a restored chain, not arbitrary
   continuous PITR. (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/restore-backup/

4. **No downgrade restore.** "Restoring a database backup to a previous Neo4j version
   is not supported." Same or later only. (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/restore-backup/

5. **Aggregation.** `neo4j-admin aggregate` consolidates a chain of backup artifacts
   into a single full artifact by applying differential transactions to the full
   store ("recovery"). Enterprise Edition only. (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/aggregate/

6. **Independent scheduling (2026.02+).** Full and differential backups can be
   scheduled on independent cadences; differential schedule drives RPO, full schedule
   drives RTO. First differential may overlap its parent full without breaking the
   chain. (3-0)
   - https://neo4j.com/docs/operations-manual/current/backup-restore/planning/

7. **Aura snapshot types.** Two types: Scheduled (auto, tier-dependent cadence) and
   On-Demand (manual via "Take snapshot"). On-Demand is the only type available to
   Free instances; Free has no scheduled/rolling backups. (3-0)
   - https://neo4j.com/docs/aura/managing-instances/backup-restore-export/
   - https://aura.support.neo4j.com/hc/en-us/articles/360037560093-How-Do-Backups-Work-in-Neo4j-Aura-
   - https://neo4j.com/cloud/platform/aura-graph-database/faq/

8. **Aura tier cadence/retention.** Professional = daily, 7-day retention; Business
   Critical = hourly scheduled + daily, 30-day; Virtual Dedicated Cloud = 60-day
   restorable / up to 90-day exportable. Pro/BC/VDC get daily full snapshots; all
   paid tiers plus Free support on-demand. (2-1 / 3-0; column semantics ambiguous)
   - https://neo4j.com/docs/aura/managing-instances/backup-restore-export/
   - https://neo4j-aura.canny.io/changelog/auradb-exportable-snapshot-format-change

9. **Aura full vs differential snapshots.** Full = entire database; differential =
   changes since last full. Differential snapshots are NOT restorable/exportable and
   cannot create new instances. (3-0)
   - https://neo4j.com/docs/aura/classic/auradb/managing-databases/backup-restore-export/

10. **Aura restore modes.** In-place restore (overwrites existing instance) or create
    a new instance from the snapshot. On demand via console or Aura API. (3-0)
    - https://aura.support.neo4j.com/hc/en-us/articles/360037560093-How-Do-Backups-Work-in-Neo4j-Aura-
    - https://neo4j.com/docs/aura/managing-instances/backup-restore-export/

11. **Aura export compatibility.** Exported files import to Community Edition.
    Importing Aura v5 snapshots into self-managed Neo4j requires 5.20+; backward v5→v4
    imports unsupported. Workflow: export as `.backup`, then `neo4j-admin database
    load`. (3-0)
    - https://neo4j-aura.canny.io/changelog/auradb-exportable-snapshot-format-change

12. **GrapheneDB.** Automatic daily (24h) backups plus on-demand snapshots, taken
    online with no downtime, retained ~1 week as downloadable tarballs, with an
    automatic consistency check before each backup is made available. Daily auto is
    plan-gated. (3-0)
    - https://docs.graphenedb.com/docs/backups

13. **Commvault.** Backs up Neo4j with initial full + incremental backups (delta of
    tx logs). Online/hot backup requires Neo4j Enterprise. Mirrors native model.
    Evidence from 2024e release only. (3-0)
    - https://documentation.commvault.com/2024e/software/managing_neo4j_backups_and_restores_using_nfs_objectstore.html

14. **GAP (derived).** Online/hot backup, incremental/differential, PITR, and chain
    aggregation are uniformly gated behind Enterprise Edition or managed services.
    Community has no native hot-backup/incremental/PITR path, only offline full
    dump/load. Commvault online also requires EE, so commercial tools inherit the gate.

## Refuted claims

- "Higher Aura tiers take more frequent scheduled snapshots than Professional:
  Business Critical hourly, VDC daily + hourly full." — 1-2.
  Source: https://aura.support.neo4j.com/hc/en-us/articles/360037560093-How-Do-Backups-Work-in-Neo4j-Aura-
- "Neo4j AuraDB provides automated backups across all service tiers." — 0-3 (Free is
  on-demand only). Source: https://neo4j.com/cloud/platform/aura-graph-database/faq/

## Open questions (unresolved by first pass)

- Encryption (at rest / in transit) and compression for backup artifacts.
- Cloud storage targets (S3 / GCS / Azure) and RBAC for backup operations.
- Causal-cluster awareness: which member is backed up, leader vs follower,
  cluster-consistent backups.
- Monitoring/alerting and configurable retention-policy automation in native tooling.
- Whether viable dedicated OSS Neo4j backup tools exist beyond logical exporters.

## Open-source tooling notes (from extraction, not all panel-verified)

- `andreshyer/neo4j-backup` — Python; downloads graph node-by-node to gzip JSON,
  uploads to a different instance; no dump files or APOC; designed for Community.
- `Akagitsunee/neo4j-community-export-tool` — Docker container, cron-scheduled bash
  exports DB as Cypher scripts to `/import` while the DB is active.
- APOC export — formats: Cypher Script, JSON, CSV, GraphML, Gephi, Excel; scope
  selectable (whole DB, specific nodes/rels, query results).
- `jexp/neo4j-shell-tools` — import/export (Cypher from CSV, GraphML, Geoff, binary)
  via neo4j-shell; not a backup product; no incremental/PITR/consistency/cluster/
  cloud/encryption/compression/scheduling/retention/monitoring.
- Ansible + `apoc.export.graphml.all` — logical export pattern, Community-compatible.

## All sources (with quality rating)

Primary:
- https://neo4j.com/docs/operations-manual/current/backup-restore/modes/
- https://neo4j.com/docs/operations-manual/current/backup-restore/restore-backup/
- https://neo4j.com/docs/operations-manual/current/backup-restore/aggregate/
- https://neo4j.com/docs/operations-manual/current/backup-restore/planning/
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
- https://neo4j.com/docs/aura/managing-instances/backup-restore-export/
- https://aura.support.neo4j.com/hc/en-us/articles/360037560093-How-Do-Backups-Work-in-Neo4j-Aura-
- https://neo4j.com/docs/aura/classic/auradb/managing-databases/backup-restore-export/
- https://neo4j.com/cloud/platform/aura-graph-database/faq/
- https://neo4j-aura.canny.io/changelog/auradb-exportable-snapshot-format-change
- https://docs.graphenedb.com/docs/backups
- https://documentation.commvault.com/2024e/software/managing_neo4j_backups_and_restores_using_nfs_objectstore.html
- https://github.com/andreshyer/neo4j-backup
- https://github.com/Akagitsunee/neo4j-community-export-tool
- https://neo4j.com/labs/apoc/4.1/export/
- https://github.com/jexp/neo4j-shell-tools

Blog/secondary:
- https://www.opcito.com/blogs/backup-and-restore-neo4j-graph-database-using-ansible

Unreliable (no usable claims):
- https://support.neo4j.com/s/article/16922797932691-Neo4j-AuraDS-Snapshots-Backups
