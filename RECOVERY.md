# Recovery

Recovery is **pure Cypher over Bolt** — no agent on the database nodes. You seed a fresh,
uniquely-named physical database from a backup artifact (`seedURI`), verify it, then
repoint the stable **alias** to it (the alias-swap cutover). The cluster pulls the seed
itself. Diagram: [restore-cutover](diagrams/restore-cutover.dot).

**The one thing to understand:** the seed artifact can be a **full** *or* a
**differential**. You point `seedURI` at a single `.backup` file; if it's a differential,
Neo4j's CloudSeedProvider finds its full + the intervening diffs in the same prefix and
applies them automatically. You never assemble the chain by hand.

## The three recovery modes

All three are the same `CREATE DATABASE … seedURI` command; they differ only in which
artifact you point at and whether you add `seedRestoreUntil`. (No `seedConfig`, no
`existingData` — those are rejected/deprecated; region + endpoint come from the server's
`AWS_REGION` / `AWS_ENDPOINT_URL_S3` env.)

### 1. Restore from a full

The latest artifact is a full (e.g. right after a full backup, no diffs yet). Recovers the
database as of that full.

```cypher
CREATE DATABASE `orders-20260629t120000`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<full>.backup' } WAIT
```

### 2. Restore from a differential chain (HEAD)

The latest artifact is a differential. Point `seedURI` at it; CloudSeedProvider resolves
the chain (full + all diffs up to it) and recovers to the newest backed-up state.

```cypher
CREATE DATABASE `orders-20260629t120500`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup' } WAIT
```

### 3. Point-in-time recovery (PITR)

Add `seedRestoreUntil` to replay the chain up to a timestamp (or transaction id). **Requires
a differential chain** — a lone full errors with *"can only be fully restored."*

```cypher
CREATE DATABASE `orders-20260629t120900`
  OPTIONS { seedURI: 's3://<bucket>/<group>/<slug>/<physical>/<diff>.backup',
            seedRestoreUntil: datetime('2026-06-29T11:00:00Z') } WAIT
```

After any of these: bring the new database online, optionally verify it, then cut over —
`ALTER ALIAS \`orders\` SET DATABASE TARGET \`orders-<ts>\``. Roll back by repointing the
alias to the previous physical; drop the old one after a soak.

## Recovery modes at a glance

| Mode | `seedURI` points at | Extra option | Recovers to | Needs a chain? |
|---|---|---|---|---|
| **Full** | a full `.backup` | — | that full's point | no |
| **Differential (HEAD)** | a differential `.backup` | — | newest backed-up state | resolves the chain |
| **PITR** | a differential `.backup` | `seedRestoreUntil: datetime(T)` | the chain replayed to **T** | **yes** |

## In the pipeline

The `restore_group` job does this for a whole group, **group-aligned** (the same
`restore_until` across every alias in the group, so referencing databases land on one
point):

- it seeds each alias from its **chain head** (`latest_artifact_key` — full or diff, the
  CloudSeedProvider resolves it), with optional `restore_until` for PITR, then
- repoints all the aliases.

Run config: `{ group_id, restore_until? }`. Locally:

```bash
just restore demo                       # mode 1/2 — restore each alias to HEAD
just restore demo "2026-06-29T11:00:00Z"  # mode 3 — group-aligned PITR
just demo-pitr                          # builds a full→change→diff chain and PITRs it
```

**Validated end to end:** `smoke_phase6` restores from a **differential** head and
reproduces the exact state; `just demo-pitr` recovers to a point inside a full→diff chain.

## The metadata layer (users, roles, privileges, aliases)

The three modes above recover **database data**. They do not recover the DBMS-wide metadata
that lives in the `system` database — users, roles, privileges, and alias definitions —
because seed-from-URI cannot target `system` (you cannot `CREATE DATABASE system`).

For that there is a separate **agentless logical export**: capture the metadata as
replayable Cypher and replay it against `system` over Bolt on a rebuilt cluster — no node
access, the same agentless surface as data restore.

- **Backup** — `metadata_export` (Dagster) / `neo4j_metadata_backup` (Airflow) writes one
  `_dbms/metadata-<ts>.cypher` artifact (SSE-KMS, same bucket).
- **Restore** — `metadata_restore` / `neo4j_metadata_restore` replays the latest (or a given
  `key`): `CREATE ROLE/USER … IF NOT EXISTS`, `GRANT ROLE …`, the `SHOW PRIVILEGES AS
  COMMANDS` statements, and `CREATE ALIAS … IF NOT EXISTS` — idempotent and additive.

**Limits (verified, by design):**

- **Native passwords are not exported.** Cypher redacts them (`SHOW USERS` → `***`) and raw
  system reads are rejected, so users are recreated with a random placeholder + `CHANGE
  REQUIRED` — reset them post-restore. SSO/LDAP users carry no local secret, so nothing is
  lost there. An *exact* password restore needs the binary `system` backup (path B).
- **Remote-alias driver credentials** are not returned by `SHOW ALIASES`; those statements
  are rendered with a `<<SUPPLY>>` placeholder and skipped on replay until supplied.
- **Alias → physical targets are a point-in-time snapshot.** In a full DR the data restore
  seeds fresh physicals and repoints user-database aliases itself; the export's `IF NOT
  EXISTS` aliases won't clobber that.

**Full-cluster restore order:** re-provision nodes → replay the metadata export (security +
aliases) → restore each user database (seed-from-URI, modes 1–3). **Validated:**
`smoke_metadata` round-trips capture → store → replay (role/user/membership/privilege/alias)
through both adapters.

## Caveats

- **PITR needs differentials.** Point-in-time is only reachable if the chain has the diffs
  covering T — so the diff cadence in your [policy](POLICY.md) is what makes PITR possible,
  not just an RPO nicety.
- **The pipeline restores the chain HEAD.** To recover an *older* point, use
  `seedRestoreUntil` with that timestamp (mode 3), not an older artifact directly.
- **Aggregation trades intra-chain PITR.** `neo4j-admin backup aggregate` collapses a
  full+diff chain into one recovered full; after that you can full-restore that point but
  lose point-in-time *within* the collapsed range. Run it on a retention cadence.
- **Non-cloud seeds** (http/ftp) or whole-store operations fall back to node-local
  `neo4j-admin database restore --restore-until` (DESIGN §3, path B).
