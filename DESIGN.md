# Neo4j Backup/Restore: Policy-Based Design for Self-Hosted Enterprise

Status: draft, 2026-06-28. Builds on [`RESEARCH.md`](RESEARCH.md) (feature matrix) and
[`second-pass-neo4j-docs.md`](second-pass-neo4j-docs.md) (verified docs deep-dive).
All sections verified against the current Neo4j operations manual and NOM docs.

Diagrams (Graphviz in [`diagrams/`](diagrams/README.md), `just diagrams` to render):
[architecture](diagrams/architecture.dot), [storage-layout](diagrams/storage-layout.dot),
[restore-cutover](diagrams/restore-cutover.dot),
[dagster-pipeline](diagrams/dagster-pipeline.dot), [naming](diagrams/naming.dot).

## 1. Context and assumptions

- Self-hosted Neo4j **Enterprise Edition**, clustered (primaries + followers).
- Multi-tenant: **multiple databases per customer**, ~50–500 customers, so the
  database count plausibly reaches the high hundreds to low thousands.
- Orchestration: **Dagster** already in house.
- Backup target: object storage (S3 assumed; `gs://`/`azb://` work identically).
- Requirement: **policy-based schedules** with per-tenant cadence/retention, plus a
  **restore loop** that closes back to verified recovery.

## 2. Data model: the database group is primary

The consistency and policy boundary is the **database group** (a set of databases
that reference each other and must be restored to a single aligned point), not the
customer. Customer is optional ownership metadata.

Three entities:

- **database** — the atomic unit. `neo4j-admin` backs up and restores one database.
  This is the Dagster unit of work.
- **database group (`db_group`)** — the consistency + policy unit. Backup cadence,
  retention, RPO/RTO, and PITR alignment all attach here. Primary entity.
- **customer / owner** — optional tag on a group, for reporting, contractual
  retention, and access filtering. Not required for backup mechanics. A customer may
  own several groups; a group belongs to one owner. If ownership is irrelevant to
  operations, it can be left out of the policy entirely and kept in a separate CRM/
  billing system.

### Registry over name-parsing

Maintain an explicit registry (`db_group` → `[databases]`, plus group policy),
reconciled against `SHOW DATABASES` on every run. Reconciliation is a feature, not
overhead: it surfaces a database with no owning group (unbacked-up risk) and a
registry entry with no live database (stale policy). Do not encode grouping in
database names and parse it back out; names drift, registries are authoritative.

## 3. Execution model: Cypher vs agents on nodes

Confirmed against the current operations manual. Headline: **the whole loop can be
node-agentless** — backup from a central runner, restore (including PITR) via Cypher
seed-from-URI that the cluster pulls itself. (Diagram:
[architecture](diagrams/architecture.dot).)

### Backup: remote-capable network client (no node agent)

`neo4j-admin database backup` "can be run both locally and remotely" and connects over
the backup port (default 6362, `server.backup.listen_address`). The machine running it
is the "backup client." Because backup "uses a significant amount of resources... it is
recommended to perform the backup on a separate dedicated machine." So a central
**backup runner host** (neo4j-admin binary + S3 creds + network reach to 6362) serves
the whole fleet. No per-node agent.

### Restore path A (preferred): Cypher seed-from-URI, cluster pulls

Run Cypher against the system database; every cluster member pulls the seed from the
URI itself. No node-local tooling.

```
CREATE DATABASE foo
  OPTIONS { seedURI: 's3://neo4j-backups/<group>/<db>/<artifact>.backup' }
-- clustered form accepts TOPOLOGY n PRIMARIES m SECONDARIES
-- NOTES (validated end-to-end):
--   * CloudSeedProvider (s3/gs/azb) does NOT accept `seedConfig`; region/endpoint
--     come from the server's AWS_REGION / AWS_ENDPOINT_URL_S3 env.
--   * `existingData: 'use'` is deprecated (removed without replacement) — omit it.
```

- URI schemes `s3:` / `gs:` / `azb:` are served by the CloudSeedProvider.
- The seed may be a **full OR differential** backup; CloudSeedProvider resolves the
  backup chain ending at the specified differential automatically.
- **PITR is supported on this path** via `seedRestoreUntil` (added 2025.01),
  for CloudSeedProvider (`s3:`/`gs:`/`azb:`) and FileSeedProvider (`file:`):

```
CREATE DATABASE foo OPTIONS {
  seedURI: 's3://neo4j-backups/<group>/<db>/<artifact>.backup',
  seedRestoreUntil: datetime('2026-06-28T18:40:32.142+0100')
}
```

  This is the key finding: arbitrary point-in-time restore does **not** require a
  node-local command for cloud seeds. Dagster drives it over Bolt.

### Restore path B (fallback): node-local `neo4j-admin database restore`

Operates on the target server's filesystem, run as the `neo4j` user, database offline
(`--to-path-data` / `--to-path-txn`), with `--restore-until` for PITR (transaction ID
or timestamp). Needed only when the seed source is not a CloudSeedProvider/
FileSeedProvider URI (e.g. http/ftp), for whole-store operations, or where you cannot
issue the Cypher path. This path requires an execution agent on the node.

### Net

| Operation | Where it runs | Agent on nodes? |
|---|---|---|
| `neo4j-admin database backup` | central runner, network to 6362 | no |
| `CREATE DATABASE ... seedURI` (restore latest) | Cypher, cluster pulls | no |
| `CREATE DATABASE ... seedRestoreUntil` (PITR, cloud seeds) | Cypher, cluster pulls | no |
| `neo4j-admin database restore --restore-until` (fallback PITR) | node-local, offline, `neo4j` user | yes |

Decision: standardize on Cypher seed-from-URI for restore (path A), keep node-local
restore as a documented fallback. The operational footprint on the database nodes is
then zero; Dagster needs only a runner host (for backup) and a Bolt connection (for
restore).

### Pure Cypher or not? (execution surface)

There is **no Cypher API for online backup** — `neo4j-admin database backup` is the
only mechanism — so backup is necessarily a CLI invocation on the runner. Restore, by
contrast, is **pure Cypher over Bolt** (seed-from-URI + `ALTER ALIAS`). So:

- "Pure Cypher everywhere" is not attainable: the backup half has no Cypher API.
- "Nothing on the database instances" *is* attainable, and is the target. The only
  execution surface is the **runner**: it runs `neo4j-admin` for backup and opens a
  Bolt connection for restore Cypher. In production the runner is the Dagster worker
  (Pipes subprocess for backup; Neo4j Python driver for restore). The instances serve
  Bolt (7687) and the backup port (6362) and host no scripts or agents.

## 4. Backup tooling

Per-database, straight to object storage:

```
neo4j-admin database backup \
  --from=follower-a:6362,follower-b:6362 \
  --to-path=s3://neo4j-backups/<db_group>/<database>/ \
  --temp-path=/var/scratch/neo4j \
  --type=AUTO --compress=true \
  <database>
```

- `--type=AUTO`: full-then-differential automatically; falls back to full if the
  required transaction logs have rotated out.
- `--from` is a comma-separated failover list, tried in order.
- `--temp-path` stages locally before the cloud upload; recommended for bucket targets.
- On 2026.02+, schedule full and differential cadences **independently**: frequent
  diffs drive RPO, periodic fulls drive RTO.
- Run `neo4j-admin backup aggregate` periodically to collapse long chains into a single
  recovered full artifact. This is the RTO lever, simplifies retention pruning, and
  produces the consistency-checkable full used in verification (§8). (The
  `neo4j-admin database aggregate-backup` name is deprecated.)
- Transaction-log retention (`db.tx_log.rotation.retention_policy`, default
  `2 days 2G`) gates differentials: if logs needed for a diff are gone, the run
  silently becomes a full. Set this in line with your diff cadence so diffs don't
  degrade unexpectedly.

### Consistency checks

`neo4j-admin database check` is expensive and should not run on a serving node. Run
it out-of-band against restored artifacts as part of restore verification (section 8),
not against the live primaries.

## 5. Source-node strategy and the 4th node

- You do not need a dedicated node to back up: resolution prefers secondaries, then
  followers, with the writer last. Start by pointing `--from` at your followers.
- The dedicated read-only secondary earns its cost as an **ops node**, not as a backup
  source per se: it absorbs backup I/O, hosts the consistency-check and restore-verify
  workloads, and doubles as a read/analytics replica.
- The decisive factor is **concurrency**. At 50–500 customers fanning out to several
  databases each, a busy cadence tick is hundreds of `neo4j-admin backup` invocations.
  Run those against a serving member and you saturate it. This is the real argument
  for a non-serving source node at the top of the range. Until then, throttle (see
  section 6) and run off-peak.

## 5.5 Runner resource model: memory and scratch (multi-TB)

`neo4j-admin database backup` stages the artifact on a local scratch path
(`--temp-path`) and then uploads to S3 — there is no stream-to-S3 mode. With
several-TB databases this scratch is the dominant constraint, so it is a configurable,
dedicated volume, never the orchestrator's disk.

### Scratch sizing and the full-vs-diff asymmetry

- A **full** backup needs scratch ≈ the database/artifact size (several TB) and holds
  it for the whole build-plus-upload window. Upload time at TB scale dominates that
  window, so the volume is occupied for a while.
- A **differential** backup stages only the transaction-log delta — small.
- `neo4j-admin backup aggregate` (chain consolidation) also reconstructs a full artifact
  and therefore also needs full-sized scratch wherever it runs.

This asymmetry drives the runner design:

- **Two lanes.** A low-concurrency *full* lane with large scratch, and a
  high-concurrency *diff* lane with small scratch. Optionally separate runner pools so
  cheap frequent diffs never queue behind a multi-TB full.
- **Concurrency bound for fulls:** `concurrent_fulls ≤ scratch_capacity /
  largest_full_size`. On a shared scratch volume that often means one big full at a
  time; stagger fulls so their multi-TB stages don't overlap.
- **Minimize fulls:** frequent diffs (RPO) + infrequent fulls (RTO) + periodic
  aggregate. Fewer full runs means less multi-TB scratch churn and upload load.
- **Scratch placement:** a sized instance-store/PVC (not tmpfs for multi-TB; tmpfs only
  if an artifact genuinely fits in RAM, which several-TB backups will not). Configured
  via `--temp-path` / `SCRATCH_PATH`, isolated from Dagster run-storage so backup bytes
  never touch the orchestrator's disk and are deleted after upload.

### Memory capping (verified)

Each backup is a short-lived JVM subprocess (Dagster Pipes); memory is per-process and
released on exit. Confirmed levers, plus one real OOM trap:

- **Heap — set `HEAP_SIZE` explicitly** (e.g. `HEAP_SIZE=2G`). If unset the JVM
  auto-sizes from "server resources"; container cgroup limits are **not** documented to
  be respected by admin tools, so do not rely on auto-detection.
- **Page cache — set `--pagecache` explicitly. This is the OOM trap.** If omitted, the
  backup inherits the server's `dbms.memory.pagecache.size`, which can be 60%+ of RAM
  and OOM the runner. Always pass `--pagecache=<size>`.
- **Consistency check is decoupled in current versions.** `database backup` no longer
  runs one (there is no `--verify` flag), so the backup is already lighter. Run
  `neo4j-admin database check` separately on the verification instance (section 8) and
  cap its `--max-off-heap-memory` (default **90% of free RAM**) and `--threads`. A check
  is "not supported for differential backups or unrecovered full backups," so verify a
  recovered/aggregated full, not a diff.
- **Upload buffers.** `dbms.integrations.cloud_storage.s3.target_throughput_gbps`
  (default `10.0`) trades throughput for more parallel S3 connections/buffers; lower it
  on the runner to cap upload memory. Keep `--parallel-download=false` (default).

Per-process RAM ≈ HEAP_SIZE + pagecache + upload buffers; runner RAM ≈ that × concurrent
jobs; scratch ≈ Σ concurrent fulls' compressed artifact sizes.

### Concrete starting budget (per backup process)

| Resource | Knob | Start | Note |
|---|---|---|---|
| JVM heap | `HEAP_SIZE` env | 2G | must set; no cgroup auto-detect for admin tools |
| Page cache | `--pagecache` | 512M-1G | MUST set or inherits server 60%+ RAM -> OOM |
| Scratch (full) | `--temp-path` | ≈ compressed DB size (multi-TB) | sized volume; omitting it stages on the install partition |
| Scratch (diff) | `--temp-path` | small (tx-log delta) | |
| Upload buffers | `s3.target_throughput_gbps` | < 10.0 to cap | runner conf / `--additional-config` |
| Consistency check | separate `database check` `--max-off-heap-memory` | cap (default 90% free) | out-of-band, recovered full only |

## 6. Orchestration with Dagster

Ship the backup tooling as its own **code location** (a dedicated gRPC code server),
dropped into the team's existing Dagster with minimal shared surface. API names below
are current (verified against docs.dagster.io). (Diagram:
[dagster-pipeline](diagrams/dagster-pipeline.dot).)

### 6.1 Why a dedicated code location

Code locations load in isolated processes — "errors in user code can't impact Dagster
system code," and one location failing to load doesn't break the others. That isolation
buys four things: independent Python deps (Neo4j driver, cloud SDKs), an independent
deploy/rollback lifecycle, dedicated run placement (multi-TB scratch, memory caps,
network to 6362), and platform/DBA ownership separate from app teams. Cost: one more
deployable image/server — worth it at this scale.

### 6.2 Definitions and resources

One `Definitions` per location:

```python
import dagster as dg

defs = dg.Definitions(
    assets=[backup_asset, prune_asset, verify_asset],
    schedules=[gold_full, gold_diff, ...],   # per-tier, per-lane
    sensors=[reconcile_registry],
    resources={
        "neo4j": Neo4jResource(...),         # Bolt: restore Cypher, alias/SHOW reconciliation
        "runner": PipesK8sClient(),          # or PipesSubprocessClient() locally
        "store": ObjectStoreResource(...),   # bucket-per-group, endpoint, region, KMS
        "policy": PolicyResource(path=...),  # loads/validates policies/*.yaml
        "naming": NamingPolicy(),            # Python mirror of bootstrap/naming.sh
    },
)
```

### 6.3 Targets, schedules, reconciliation sensor

- **Dynamic partitions** keyed by `(group, alias)`:
  `targets = dg.DynamicPartitionsDefinition(name="backup_targets")`.
- **Reconciliation sensor** syncs partitions from the policy registry vs
  `SHOW DATABASES`/`SHOW ALIASES`, emitting partition mutations and runs together:
  ```python
  return dg.SensorResult(
      run_requests=[dg.RunRequest(partition_key=k) for k in due],
      dynamic_partitions_requests=[targets.build_add_request(new),
                                   targets.build_delete_request(stale)],
  )
  ```
  Drift it surfaces: an unbacked-up database, a stale policy entry, an orphan alias.
- **Tiered schedules** emit `RunRequest(partition_key=...)` for due partitions; a group
  co-schedules on one tick so PITR aligns.

### 6.4 Run placement (multi-TB runner) in Kubernetes

Baseline once per location, override per backup job.

- **Per location** — `container_context.k8s.run_k8s_config` (Dagster+) or the OSS
  `K8sRunLauncher.run_k8s_config`: namespace, node pool, default resources.
- **Per job** — the `dagster-k8s/config` tag attaches the sized scratch PVC, memory
  limit, and node pool to that backup's pod:
  ```python
  tags={"dagster-k8s/config": {
      "container_config": {"resources": {"limits": {"memory": "4Gi"}},
                           "volume_mounts": [{"name": "scratch", "mount_path": "/scratch"}]},
      "pod_spec_config": {"node_selector": {"workload": "neo4j-backup"},
                          "volumes": [{"name": "scratch",
                                       "persistentVolumeClaim": {"claimName": "..."}}]},
  }}
  ```

### 6.5 Execution via Pipes

`neo4j-admin` runs as an external process with its real exit code surfaced to the step:

- **`PipesK8sClient`** (production) — each backup launches its own pod with a fresh,
  right-sized scratch PVC, then tears down. Ideal for multi-TB fulls and the lanes:
  ```python
  from dagster_k8s import PipesK8sClient
  client.run(context=context, image=NEO4J_IMAGE,
             command=["neo4j-admin","database","backup","--from",src,
                      "--to-path",f"s3://{bucket}/{group}/{slug}/",
                      "--temp-path","/scratch","--pagecache",pc,
                      "--type","AUTO","--compress=true", phys],
             base_pod_spec={...scratch PVC + node_selector...}).get_materialize_result()
  ```
  (`HEAP_SIZE` goes in the pod's container env.)
- **`PipesSubprocessClient`** (local/in-pod) — `client.run(command=[...],
  context=context).get_materialize_result()`. The demo's shape. A non-zero exit fails
  the step; `--keep-failed` preserves the artifact for triage.
- **Restore does NOT use Pipes** — it is the `neo4j` Bolt resource issuing seed-from-URI
  + `ALTER ALIAS` Cypher (section 7). Pipes is for the backup CLI only.

### 6.6 Concurrency: lanes + DB protection

- **Full vs diff lanes** via run-tag limits in `dagster.yaml` — full bounded by scratch
  capacity, diff higher:
  ```yaml
  concurrency:
    runs:
      tag_concurrency_limits:
        - {key: backup_kind, value: full, limit: 1}
        - {key: backup_kind, value: diff, limit: 6}
  ```
  Tag each run `backup_kind: full|diff`.
- **Protect the Neo4j source** with a concurrency **pool** so backups don't pile onto
  one member: `@dg.asset(pool="neo4j")` + `dagster instance concurrency set neo4j N`
  (set `concurrency.pools.granularity: run` to cap whole runs, not just ops).

### 6.7 Downstream + idempotency

- **Downstream assets:** retention pruning and `aggregate` run downstream of the backup
  asset, per group, keyed off `retention_days`. (`aggregate` needs full-sized scratch —
  §5.5.)
- **Metadata per run:** database, group, type, artifact URI, last txn id/timestamp,
  duration, bytes — feeds retention and PITR-aligned restore.

### 6.8 Dropping into an existing deployment

Additive:

1. Add one code-location entry — `python_module` in `workspace.yaml` (OSS, with
   `executable_path` for a separate venv) or a `locations:` entry with its own `image`
   in `dagster_cloud.yaml` (Dagster+). Existing locations untouched.
2. Deploy the code server with its own secrets/env (Neo4j, AWS, KMS) and placement.
3. Add the scratch PVC, node pool, and network policy (reach 6362 + object store).
4. Point `PolicyResource` at the policy source.
5. Enable the schedules + reconciliation sensor.

Shared-surface caveats — the honest friction: run storage, event log, and the daemon
(scheduler/sensors/run queue) are instance-global, so the lane `tag_concurrency_limits`
live in the shared `dagster.yaml`; coordinate that one change with whoever owns the
instance. And pin the location's `dagster` version close to the host webserver/daemon —
cross-version skew is not a documented guarantee.

### Why Pipes (not a bare op)

`neo4j-admin` is a long external process with meaningful exit codes and logs. Pipes
launches it and streams logs/metadata/materialization back with the real exit status,
keeping orchestration in Dagster and execution on the runner that has the binary, creds,
scratch, and network path.

## 7. Restore loop

Restore is organized at the **group** level so a tenant's referencing databases come
back aligned, driven over Cypher (section 3, path A) so no node agent is involved, and
cut over via **database aliases** for a near-zero-downtime, instantly-reversible swap.
(Diagrams: [restore-cutover](diagrams/restore-cutover.dot),
[naming](diagrams/naming.dot).)

### Alias-swap cutover (production restore strategy)

Applications connect to a stable **alias** (e.g. `acme-orders`), never to a physical
database. Each restore seeds a fresh, uniquely-named physical database and repoints the
alias. All of this is Cypher over Bolt (section 3).

**Naming authority.** Alias and database names obey *different* Neo4j rules, and teams
already depend on the alias freedom, so naming is owned by one module
(`bootstrap/naming.sh`, mirrored by the orchestrator's NamingPolicy). Three identifiers:

- *Alias* — the team's app-facing name. Validated against Neo4j's **full** alias spec
  (up to 65534 chars; almost any character if backtick-quoted; no trailing dot; no
  `_`/`system` prefix; a dot denotes a composite constituent) and **preserved exactly**.
  Existing aliases are never force-renamed.
- *Slug* — a deterministic, db-legal, path-safe id derived from the alias (clean
  aliases pass through; messy ones become sanitized + an 8-char hash so distinct
  aliases never collide). Used for object-storage prefixes and as the physical-name base.
- *Physical* — `<slug>-<ts>`: the unique standard database a restore seeds into. Always
  a legal database name (`[a-z0-9.-]`, 3-63, lowercase, no underscores).

Steps:

1. **Seed into a unique name.** `CREATE DATABASE \`<slug>-<ts>\` OPTIONS { seedURI:
   's3://<group-bucket>/<slug>/<artifact>.backup' } WAIT`. Unique naming sidesteps the
   can't-reseed-an-existing-database rule; the live database keeps serving while the
   restore materializes. `WAIT` blocks until cluster members host it. (No `seedConfig`
   for CloudSeedProvider — region/endpoint via server env; `existingData` is deprecated,
   omit it.)
2. **Group-aligned PITR.** The same `seedRestoreUntil: datetime('<T>')` across every
   database in the group lands them on one wall-clock instant (bounded by clock skew and
   transaction boundaries) — the closest native group-consistent snapshot. **Validated
   constraint:** `seedRestoreUntil` requires a backup **chain** (a full plus the
   differential(s) covering T); a standalone full errors with "can only be fully
   restored." So PITR depends on the differential cadence existing — diffs are not just
   an RPO optimization, they are what make point-in-time targets reachable.
3. **Verify before cutover.** Bring each `<slug>-<ts>` online and run a consistency
   check + smoke queries (section 8) while the alias still points at the old database.
4. **Cut over.** `ALTER ALIAS \`<alias>\` SET DATABASE TARGET \`<slug>-<ts>\`` for every
   alias in the group, after all seeds succeed. Requires the `ALTER ALIAS` (or umbrella
   `ALIAS MANAGEMENT`) DBMS privilege. Verified caveats: in-flight transactions running
   against the alias are **aborted and rolled back** at repoint (not a graceful drain —
   applications must retry); on a cluster the change is written to the system database
   and reconciled to members **asynchronously**, so allow a brief window where members
   may resolve the alias differently.
5. **Roll back / clean up.** Rollback is repointing the alias to the previous physical
   database. After a soak, `DROP DATABASE` the superseded one.

Differentials are fine as the seed (CloudSeedProvider resolves the chain ending at the
named differential). Fallback to node-local `neo4j-admin database restore
--restore-until` only for non-cloud seed sources or whole-store operations (section 3,
path B). Cross-group transactional consistency is not provided by native backup; if a
tenant needs true atomic consistency across databases, that requires app-level
quiescing or accepting the bounded skew. State this explicitly to stakeholders.

### Why this shape

The alias indirection turns restore from a destructive in-place operation into an
out-of-band build plus a pointer move. It composes cleanly with the rest of the design:
seed-from-URI builds the new database with zero node agents, group-aligned
`seedRestoreUntil` keeps the set consistent, and the verification step (section 8)
gates the swap so you never cut over to an unverified restore.

## 7.5 Backup-file encryption (per-group key)

Requirement: backup files encrypted with a configurable key, possibly per group.
`neo4j-admin` does **not** encrypt artifacts at rest (only TLS in transit; the stored
file is the operator's responsibility). So this is added in the pipeline, and there is
a real fork because it interacts with the agentless restore path.

### Option A — server-side SSE-KMS at the object store (recommended default)

The store encrypts on PUT under a KMS key; `neo4j-admin` writes plaintext over TLS and
the bucket/KMS config does the rest. Restore via seed-from-URI still works unchanged,
because any caller with KMS decrypt permission reads cleartext transparently. The DB
nodes already have S3 access for seeding; grant them KMS decrypt too.

- Pro: keeps the whole loop node-agentless; no decrypt step; minimal pipeline change.
- Con: "per-group key" granularity with bucket default encryption realistically means
  **one bucket (or KMS key) per group**, since S3 default encryption is bucket-wide and
  `neo4j-admin` is unlikely to send per-object SSE headers. Workable at ~50-500 groups
  but it multiplies buckets/keys.
- Threat model covered: bucket compromise, at-rest disclosure, key revocation per
  group. Not covered: a storage admin who also holds KMS decrypt.

### Option B — client-side envelope encryption, per-group key

The runner encrypts the artifact (per-group KEK wrapping a per-file DEK; age/gpg/KMS)
before upload. The artifact is opaque everywhere, independent of the store.

- Pro: strongest; true per-group (or per-arbitrary-scope) keys; store never sees
  plaintext; works against any object store including plain MinIO.
- Con: **breaks seed-from-URI restore** — the server would pull ciphertext it cannot
  read. Restore becomes download to the runner, decrypt, then either `file://` seed or
  node-local `neo4j-admin database restore`. That reintroduces a node-local/staging
  step and gives up the clean Cypher restore path.

### Decision: SSE-KMS, keys in cloud KMS

Chosen because **seed-from-URI restore is a hard requirement** and SSE-KMS is the only
option that preserves it. Mechanism:

- The encryption is server-side at the **object store**, applied by the team's
  bucket/IAM/KMS structure — **not** something the pipeline imposes. Because S3 SSE-KMS
  *default encryption* is bucket-wide, the common way to bind a distinct key per group
  is a bucket per group; a shared bucket with one key works too. Either way the pipeline
  is object-store-agnostic: it takes a configurable bucket and writes
  `<group>/<slug>/<physical>/` prefixes. The policy *carries* `s3_prefix` and
  `kms_key_ref` per group so teams that want per-group buckets/keys can wire them; the
  pipeline does not require it.
- `backup.sh` and `restore.sh` stay unchanged: the store encrypts on PUT and decrypts
  on GET transparently for callers with KMS decrypt. The encrypt/decrypt hooks in
  those scripts remain no-ops, which is the point of choosing SSE-KMS.
- The DB nodes already pull seeds from object storage; grant them **KMS decrypt** on
  the relevant per-group keys so seed-from-URI reads cleartext.
- Rotation/revocation is per group via the KMS key. Audit is the KMS key's audit trail.

Local dev stands in for cloud KMS with MinIO's built-in KMS (a single demo key, since
multiple keys need MinIO KES + a real KMS backend). The mechanism (bucket default
SSE-KMS, transparent read on seed) is faithful; only the per-group-key multiplicity is
simplified locally.

Open sub-item being verified now: whether the server-side seed pull works against a
plain-HTTP MinIO or requires TLS, and that SSE-KMS objects restore transparently via
seedURI. This gates the critical path, so it is being confirmed against the docs rather
than assumed.

## 8. Restore verification (closing the loop)

Backups are only real once they are proven. Validated approach (simpler than restoring
into an ephemeral instance — `database check` reads the artifact directly):

1. **Aggregate** the chain into a recovered full: `neo4j-admin backup aggregate
   --from-path=s3://<bucket>/<prefix>/ --temp-path=<scratch> <database>`. This collapses
   full + differentials into one recovered full **in place** (default removes the old
   chain; `--keep-old-backup=true` to retain). It is also the RTO/retention lever (§4).
   Note it trades intra-chain PITR for a compact full, so aggregate per retention policy,
   not casually — and to verify without mutating the production store, aggregate a copy
   in a scratch bucket.
2. **Check** the recovered full directly from object storage: `neo4j-admin database
   check --from-path=s3://<bucket>/<prefix>/<full>.backup --temp-path=<scratch>
   --max-off-heap-memory=<cap> <database>`. No seed/stop/drop. Exit 0 + no report =
   consistent; non-zero exit + an `inconsistencies-<ts>.report` = fail. `check` refuses
   differentials and unrecovered fulls, which is why step 1 comes first.
3. **Record** pass/fail against the group's RPO/RTO; alert on failure or staleness.

`check` is the memory-heavy, out-of-band operation — cap `--max-off-heap-memory` (it
defaults to 90% of free RAM) and run it on the runner/ops node, never a serving member.
In Dagster this is the `aggregate_chain` and `verify` jobs (validated end-to-end via
`orchestrator/smoke_verify.py`).

> **Storage layout (implemented + validated).** A backup chain is **per physical
> store**, so the layout is `<group>/<slug>/<physical>/<artifact>.backup`: each
> `<physical>/` directory is one store (a valid chain), and the `<slug>/` level groups
> an alias's physicals so "latest backup for the alias" is the newest artifact across
> them. Repeated backups of an alias between restores chain correctly (same physical);
> a restore starts a fresh physical prefix. `aggregate` and `verify` are partitioned by
> `(group, alias)` and resolve the live physical from the latest key. Verified end to
> end via `orchestrator/smoke_phase6.py`. (Diagram:
> [storage-layout](diagrams/storage-layout.dot).)

## 9. Capacity and scaling

- 50–500 customers × multiple databases can reach four-figure database counts. A
  single DBMS becomes memory- and checkpoint-bound well before "unlimited"; budget
  page cache and heap per database against your Neo4j version's many-databases
  guidance.
- If you approach ~1000 databases, **shard customers/groups across multiple DBMS
  clusters**. This also spreads backup load, compounding with the concurrency control
  in section 6. The registry and tiering model carry over unchanged; the runner just
  targets multiple clusters' `--from` lists.

## 10. Neo4j Ops Manager (NOM) evaluation

Confirmed: **NOM provides nothing usable for backup orchestration today.** Use
Dagster; do not wait on NOM.

- **No backup feature in any released version** through 1.15.2 (May 2026). NOM's four
  pillars are Monitoring, Administration, Operation, Integration; the only shipped
  Operation feature is the Upgrade Manager. A `backup-manager` page exists in the docs
  navigation but is **commented out / unreleased**. "Backups" appears only as
  aspirational language in the launch blog, not as a feature.
- **No public API to drive it.** NOM's API is an internal, token-authorized GraphQL
  endpoint, explicitly "not yet public." No REST API. An external orchestrator has no
  supported way to drive NOM.
- **Per-node agent.** NOM deploys a Go agent on each Neo4j host (gRPC to the server)
  that runs DBMS and OS commands locally. Note the irony: NOM would put an agent on
  every node, whereas the seed-from-URI restore path (section 3) needs **zero** node
  agents. NOM does not reduce your footprint.
- **Free** for Enterprise license holders (includes one dedicated DBMS for NOM's own
  use); 30-day eval without a license.

**Where NOM still fits (optional, orthogonal to backup):** it is a solid free
monitoring/alerting and upgrade-planning layer for the cluster itself. You could run
NOM for cluster health (host/JVM/database metrics, alerts, log management) while
Dagster owns the backup/restore loop. The two do not overlap. If you adopt NOM, surface
backup outcomes in your own alerting (from Dagster), not NOM, since NOM has no backup
signal. Revisit if/when `backup-manager` ships.

## 11. Policy / registry schema (starter)

Group-centric. Customer is an optional `owner` tag.

```yaml
db_groups:
  - id: acme_core
    owner: acme                 # optional; reporting/retention-by-contract only
    databases: [acme_orders, acme_graph, acme_audit]
    tier: gold
    s3_prefix: s3://neo4j-backups/acme_core/
    retention_days: 30
    rpo_minutes: 60
    rto_minutes: 120
    overrides:
      acme_audit: { tier: silver }   # per-database cadence override

tiers:
  gold:   { full_cron: "0 2 * * *",   diff_cron: "0 * * * *" }    # daily full, hourly diff
  silver: { full_cron: "0 3 * * 0",   diff_cron: "0 */6 * * *" }  # weekly full, 6h diff
  bronze: { full_cron: "0 4 * * 0",   diff_cron: "0 0 * * *" }    # weekly full, daily diff
```

## 12. Decisions and remaining choices

Resolved:

- **Restore path: Cypher seed-from-URI (path A).** PITR is supported on it via
  `seedRestoreUntil` for cloud seeds, so the earlier assumption that PITR forces a
  node-local command was wrong. Node-local `neo4j-admin restore` is a fallback only.
  The whole loop is node-agentless.
- **NOM: out of the backup loop.** No backup feature, no public API. Optionally keep it
  for cluster monitoring/upgrades only (section 10).
- **Ownership/customer: optional tag, not a driver.** The `db_group` is the policy and
  PITR-alignment unit; `owner` is reporting metadata that can live entirely outside the
  policy if preferred.

Still to choose:

- Where the **backup runner** lives: the dedicated ops node, or a separate runner host
  with network reach to 6362. Either works since backup is a remote-capable client.
- Whether to provision the **dedicated read-only secondary** now or defer until backup
  concurrency justifies it (section 5).
- **Multi-cluster sharding** threshold if database counts approach ~1000 (section 9).
