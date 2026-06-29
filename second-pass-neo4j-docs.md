# Second Pass: Closing the Open Questions (Neo4j docs deep-dive)

Conducted 2026-06-28 against official Neo4j documentation to resolve the five
feature categories the first pass could not confirm: encryption, cloud storage
targets, RBAC, cluster awareness, and monitoring/retention. All quotes verbatim
from neo4j.com/docs unless noted.

The headline correction: the first pass marked cloud storage targets, compression,
and in-transit encryption as "unconfirmed." Native Neo4j supports all three directly.

---

## 1. Encryption & compression

### Compression — CONFIRMED
`neo4j-admin database backup` supports `--compress[=true|false]`, **default `true`**.
> "Request backup artifact to be compressed. Compression can yield a backup artefact
> many times smaller, but the exact reduction depends upon many factors..."
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/

### Encryption in transit — CONFIRMED
A dedicated SSL policy scope **`backup`** exists (port 6362). Scopes: `bolt` (7687),
`https` (7473), `cluster` (6000/7000/7688), `backup` (6362).
> "Set the backup SSL policy to `true`: `dbms.ssl.policy.backup.enabled=true`"
> "...client authentication to `REQUIRE` to enable the mutual authentication..."
- https://neo4j.com/docs/operations-manual/current/security/ssl-framework/

### Encryption at rest (self-managed artifact) — NOT documented as built-in
Docs describe protecting the artifact via SSL + firewall only; no built-in at-rest
encryption of the produced artifact file. Securing the stored file is the operator's
responsibility (filesystem/storage layer).
> "Securing your backup network communication with an SSL policy and a firewall
> protects your data from unwanted intrusion and leakage."
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/

### Aura encryption — CONFIRMED (at rest + in transit)
> "...encrypted at rest using the underlying cloud provider's encryption mechanism."
> "...AWS SSE-S3 ... Azure Storage Encryption (SSE), or Google-managed encryption.
> This ensures all your data ... uses 256-bit Advanced Encryption Standard (AES)."
> "Aura always requires encrypted connections... network traffic flowing to and from
> Neo4j Aura is always encrypted."
Optional Customer Managed Keys (CMK) via the cloud provider's KMS.
- https://neo4j.com/docs/aura/security/encryption/

---

## 2. Cloud storage targets — CONFIRMED (major first-pass correction)

`neo4j-admin database backup --to-path` AND `neo4j-admin database dump --to-path`
write directly to **AWS S3, Google Cloud Storage, and Azure Blob Storage**.

> "The `--to-path=<path>` option can also back up databases into AWS S3 buckets,
> Google Cloud storage buckets, and Azure buckets."

URI schemes:
- S3: `--to-path=s3://myBucket/myDirectory/`
- GCS: `--to-path=gs://myBucket/myDirectory/`
- Azure: `--to-path=azb://myStorageAccount/myContainer/myDirectory/`

S3-compatible endpoints supported via endpoint override (AWS SDK v2):
> "...override the endpoints so that the AWS SDK can communicate with alternative
> storage systems, such as Ceph, Minio, or LocalStack."

Recommendation: provide `--temp-path` when the target is a cloud bucket.

Credentials per cloud:
- AWS: `~/.aws/credentials` + `~/.aws/config` (`aws configure set ...`)
- GCS: `GOOGLE_APPLICATION_CREDENTIALS` (JSON key) + `GOOGLE_CLOUD_PROJECT`
- Azure: default Azure credentials via `az login` (no SAS/env-var setup documented)

A configuration category "Cloud storage integration settings" was added in 2025.03.
There is no standalone cloud-storage backup subpage; usage is documented inline per
command.
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
- https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/

---

## 3. RBAC for backup/restore

### Self-managed neo4j-admin — OS-level, no Cypher RBAC — CONFIRMED
Backup/restore/dump/load are command-line tools authenticated by OS/filesystem
access, not database (Cypher) roles.
> "The command must be invoked as the `neo4j` user to ensure the appropriate file
> permissions."
> "`neo4j-admin database restore` must be invoked as the `neo4j` user..."
No backup/restore DBMS privilege exists. The DBMS privilege categories are ROLE/USER/
IMPERSONATE/AUTH RULE/DATABASE/ALIAS/SERVER/PRIVILEGE MANAGEMENT, EXECUTE, SETTING.
`ALL DBMS PRIVILEGES` does **not** include any backup/restore capability.
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
- https://neo4j.com/docs/operations-manual/current/authentication-authorization/dbms-administration/

### Aura — console project RBAC — CONFIRMED
Snapshot/restore actions are gated to PROJECT_ADMIN. Roles: PROJECT_VIEWER,
METRICS_READER, PROJECT_MEMBER, PROJECT_ADMIN. PROJECT_ADMIN-only capabilities:
"Take on-demand snapshots", "Restore from snapshots", "Download/Export snapshot".
- https://neo4j.com/docs/aura/user-management/

### GrapheneDB — account/org roles (Owner/Admin/Collaborator), not DB RBAC.
### Commvault — granular RBAC; explicit backup and restore permissions via roles +
security associations.

---

## 4. Cluster-aware backup — CONFIRMED

- Back up from **any** member; each server has two configurable backup ports.
  > "...it is possible to take a backup from any server hosting the database to
  > backup, and each server has two configurable ports capable of serving a backup."
- Prefer **secondaries** (they outnumber primaries). Automatic resolution order
  (`--remote-address-resolution`): **secondary → primary follower → primary writer
  (last)**, keeping the writer/leader as last resort.
  > "...selects all servers hosting the database in secondary mode. If it is not
  > possible to back up from one of the secondaries, the DBMS attempts to take a
  > backup from the primary followers before finally trying the database primary
  > writer."
- `--from` is a **comma-separated host:port list, tried in order**; supplying multiple
  servers is recommended for resilience. Default backup port **6362**
  (`server.backup.listen_address`).
- Backup is a **single-member operation**. A full backup **forces a checkpoint**
  before proceeding. The docs do **not** claim cluster-wide transactional consistency
  (not stated either way).
- Restore into a cluster via **seed-from-URI** (identical seed to all servers) or a
  **designated seeder** (`neo4j-admin database restore` to one server, then it seeds
  others). A seed can be a full backup, differential backup, or dump. Topology can be
  **redefined** on restore.
  > "When restoring a database from a backup seed in a cluster, you have to define the
  > database topology... create a topology that will differ from the original one."
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
- https://neo4j.com/docs/operations-manual/current/clustering/databases/

---

## 5. Monitoring/alerting & retention

### Monitoring/alerting — NO built-in metrics/alerting
Backup outcome is reported via **exit codes + logs** only; the Metrics Reference
contains no backup metrics.
- Exit `0` = success; `1` = failed or succeeded-with-problems (e.g. uncontactable
  servers). Multi-db codes follow the same pattern.
- `--keep-failed` (default `false`) preserves a failed backup directory for analysis.
- No native success/failure/duration/last-backup metric; detection is exit codes +
  log inspection + external orchestration.
- https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/
- https://neo4j.com/docs/operations-manual/current/monitoring/metrics/reference/

### Retention automation — NONE native (operator-managed)
No auto-pruning of backup artifacts or chains. Planning page lists "how many backups
to keep" as an operator decision; no automation feature exists.
- https://neo4j.com/docs/operations-manual/current/backup-restore/planning/
- https://neo4j.com/docs/operations-manual/current/backup-restore/modes/

### Transaction-log retention gates differentials — CONFIRMED
`db.tx_log.rotation.retention_policy`, default **`2 days 2G`**. If logs needed for a
differential are pruned/rotated out, the backup client falls back to a full backup.
> "This option [keep_none] is not recommended in production Enterprise Edition
> environments, as differential backups rely on the presence of the transaction logs
> since the last backup."
(Supersedes the legacy `keep_logical_logs` parameter.)
- https://neo4j.com/docs/operations-manual/current/database-internals/transaction-logs/

### Aura — tier-fixed retention, no failure alerting documented
Retention is tier-determined with no operator control. No backup-failure
alerting/notification is documented.
- https://neo4j.com/docs/aura/managing-instances/backup-restore-export/

---

## Updated status of first-pass open questions

| Open question | Resolution |
|---|---|
| Encryption at rest / in transit | In transit: confirmed (`backup` SSL scope; Aura TLS). At rest: Aura yes (256-bit AES + optional CMK); self-managed artifact has no built-in at-rest encryption. |
| Compression | Confirmed: native `--compress`, default true. |
| Cloud targets (S3/GCS/Azure) | Confirmed: native backup AND dump write to `s3://`, `gs://`, `azb://`; S3-compatible (Ceph/Minio/LocalStack). |
| RBAC | Self-managed = OS-level only (no Cypher privilege). Aura = console PROJECT_ADMIN. GrapheneDB = account roles. Commvault = granular RBAC. |
| Cluster awareness | Confirmed: any-member backup, secondary-preferred resolution order, `--from` failover list, single-member (no cluster-wide consistency claim), seed-from-URI / designated-seeder restore with topology redefinition. |
| Monitoring/alerting | No native metrics/alerting; exit codes + logs + `--keep-failed` only. |
| Retention automation | None native (operator-managed); tx-log retention `2 days 2G` default gates differentials. Aura tier-fixed. |
