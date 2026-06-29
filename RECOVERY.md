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
