# Policy

The policy is a YAML file listing the database groups, the schedule tier each group uses,
and how long backups are kept. Every job reads it.

- Loaded from `NEO4J_BACKUP_POLICY` (a path or `s3://bucket/key.yaml`).
- Schema: `orchestrator/neo4j_backup_core/policy.py`.
- Checked on load: unknown keys, aliases, tiers, crons, names and storage prefixes. Check a
  file before shipping it with `neo4j-backup policy validate <file>`.

## Policy or environment

The policy says what each customer gets; the deployment's environment says how this
installation reaches things. As a rule of thumb:

| In the policy | In the environment |
|---|---|
| groups, members, tiers and schedules, retention, `rpo_minutes` | the policy's location (`NEO4J_BACKUP_POLICY`, `POLICY_CACHE_TTL`, `POLICY_LOADER`) |
| profiles, tenants, `load:` (mode, writes, build, checks) | the `default` cluster and store when the policy doesn't define them (`NEO4J_BOLT_URI`, `NEO4J_BACKUP_SOURCE`, `BACKUP_BUCKET`, `CLOUD`, `AWS_REGION`, ...) |
| named `clusters:` and `storage:` with secret references | secrets themselves (`NEO4J_PASSWORD`, `SECRET_PROVIDER`, cloud credentials) |
| `naming:` and per-member `overrides` | runner settings (`RUNNER_PAGECACHE`, `SCRATCH_PATH`, `RUNNER_EXEC_PREFIX`), `BACKUP_UPLOAD` |
| `dbms:` crons, `schedules: running` | hooks (`PATH_LAYOUT`, `NAMING`, `CUTOVER_STRATEGY`), `NEO4J_BACKUP_REQUIRE_TICKET` |

The full environment list is in [ENVIRONMENT.md](ENVIRONMENT.md).

## A complete policy

Copy this, delete what you don't need, and edit it. Every field is shown. Fields marked
**[not read]** are accepted but change nothing.

```yaml
# Required: db_groups (a list) and tiers (a map). Optional: storage, profiles, dbms, schedules.

storage:                            # named stores; omit for the environment's one store
  finance:
    url: s3://neo4j-backups-finance # s3://<bucket> | gs://<bucket> | azb://<account>/<container>
    sse_kms_key_id: alias/finance-backups

db_groups:
  # ----- one entry per backup group (the policy + PITR-alignment unit) -----
  - id: acme                        # group id; part of the storage path and partition key
    aliases:                        # app-facing Neo4j aliases to back up (one unit each)
      - acme-orders
      - acme-graph
      - acme-audit
    tier: gold                      # must match a key under `tiers:` below
    retention_days: 30              # prune deletes whole chains older than this (see below)

    tenant: acme-corp               # label for selecting a customer's groups (--tenant)
    storage: finance                # the named store for this group's backups
    rpo_minutes: 90                 # `status` reports a member stale when its newest backup is older
                                    # (default: the tier's rpo_minutes, else 60)
    rto_minutes: 120                # how long a restore waits for a seed before failing
    encryption:                     # [not read] use a named store's sse_kms_key_id instead
      mode: sse-kms
      kms_key_ref: acme-key
    topology:                       # cluster shape a restore seeds into (omit on standalone)
      primaries: 3                  #        CREATE DATABASE … TOPOLOGY 3 PRIMARIES 0 SECONDARIES
      secondaries: 0
    overrides: {}                   # per alias: topology, prefix, physical_base, adopt_from
    load:                           # how data arrives and what is kept (omit: live)
      mode: live                    #        live | once | rebuild
      writes: read-write            #        read-write | read-only
      protect_writes: true          #        back up writes made after a load
      keep_loads: 14                #        load artifacts kept restorable
      baseline: true                # full backup right after a restore or load

  # ----- a group rebuilt from source files every night, read-only in between -----
  - id: catalog
    aliases: [catalog]
    tier: rebuild_nightly
    load:
      mode: rebuild
      writes: read-only
      keep_loads: 14
      source: { storage: default, prefix: catalog/_source/ }   # default: <group>/_source/
      build:
        kind: import                # import | command (a command or container in the runner)
        input_type: parquet         # csv | parquet (import only)
        args: [--nodes=Product=products/*.parquet, --relationships=IN=in/*.parquet]

  # ----- a second group on a lighter tier (minimal: only the required fields) -----
  - id: globex
    aliases: [globex-main]
    tier: bronze
    retention_days: 7

# ----- named schedules referenced by db_group.tier -----
tiers:
  gold:
    full_cron: "0 2 * * *"          #   daily full at 02:00 (UTC in Dagster)
    diff_cron: "0 * * * *"          #   hourly differential
    rpo_minutes: 90                 #   the default for the tier's groups
  bronze:
    full_cron: "0 4 * * 0"          #   weekly full (Sundays 04:00)
    diff_cron: "0 0 * * *"          #   daily differential
    rpo_minutes: 1500
  rebuild_nightly:                  # a tier for load-only groups may have only rebuild_cron
    rebuild_cron: "0 1 * * *"       # when rebuilds run; no backups, so no point-in-time restore
    rpo_minutes: 1500
```

The smallest valid group needs only `id`, `aliases` and `tier`.

## Profiles

A platform team serves the same kinds of customer again and again. `profiles:` names a set of
group settings once; a group points at one with `profile:` and sets only what differs.

```yaml
profiles:
  app-live:       { tier: gold, retention_days: 14 }
  data-interval:  { tier: rebuild_nightly, load: { mode: rebuild, writes: read-only, keep_loads: 14 } }
  data-scratch:   { tier: rebuild_nightly, load: { mode: rebuild, writes: read-write, protect_writes: false, keep_loads: 7 } }
  data-then-live: { tier: gold, load: { mode: once, writes: read-write, protect_writes: true } }

db_groups:
  - id: recs-sandbox
    profile: data-scratch
    aliases: [recs]
    load: { keep_loads: 3, build: { kind: import, input_type: csv, args: [--nodes=Item=items.csv] } }
```

- A profile may set any group field except `id`, `aliases`, `databases` and `overrides`.
- The group's own values win. `load:` merges key by key; everything else, lists included
  (`build.args`), is replaced.
- Profiles are applied before validation, so a profile can supply required fields such as
  `tier`. An unknown profile, or an unknown field in a profile, fails policy load.
- Every front-end (Dagster, Airflow, the CLI, MCP) sees the resolved groups.
- `neo4j-backup policy show <group>` prints the effective settings and the source of each
  (`group`, `profile:<name>`, `default`). `neo4j-backup policy validate <file>` checks a file
  without touching a cluster or store.

The four types in `policies/demo.yaml`: `app-live` (an application writing live, backed up on
`gold`), `data-interval` (rebuilt every night from source files, read-only), `data-scratch`
(rebuilt every night, writes allowed but not kept), `data-then-live` (loaded once, then written
live and backed up). A group on `data-interval` or `data-scratch` adds its own `load.build`.

## Clusters

One policy covers every Neo4j cluster. The `default` cluster comes from the environment
(`NEO4J_BOLT_URI`, `NEO4J_USER`, `NEO4J_PASSWORD_REF`, `NEO4J_BACKUP_SOURCE`, …) unless
`clusters:` defines it. A group picks one with `cluster:`.

```yaml
clusters:
  us-east:
    bolt_uri: neo4j://neo4j-use1.internal:7687
    user: neo4j
    password_ref: aws-sm:neo4j/use1         # secret reference (see Storage)
    backup_source: [use1-a:6362, use1-b:6362]
    storage: default                         # the cluster's store; a group's `storage` wins
    topology: { primaries: 3, secondaries: 0 }   # default for its groups
    cypher_version: "5"
    cutover: { strategy: alias-swap }        # or external, with `hook`
    backup_upload: admin                     # or pipeline
db_groups:
  - id: orders
    cluster: us-east
    aliases: [orders]
    tier: gold
```

Group ids are unique across the policy, and an alias or database belongs to one group per
cluster (the same name on two clusters is fine). Store precedence: the group's `storage`, then
its cluster's, then `default`. `targets` and the MCP status show each group's cluster.

DBMS-wide jobs run per cluster, on the `dbms:` crons: the metadata export and the `system`
backup (Dagster `<cluster>_dbms_metadata` / `<cluster>_dbms_system`, Airflow
`neo4j_dbms_metadata_<cluster>` / `neo4j_dbms_system_<cluster>`, CLI `metadata export|restore
--cluster` and `system-backup --cluster`). The default cluster keeps the original keys
(`_dbms/metadata-<ts>.cypher`, `_dbms/system/`); a named cluster writes under `_dbms/<cluster>/`
in its store. Listings use exact prefixes, so no cluster sees another's exports; the cluster
names `system` and `metadata…` are reserved for that reason.

Concurrency per cluster: Dagster runs started by the schedules carry a `neo4j_cluster` tag
(limit it in `dagster.yaml` with `applyLimitPerUniqueValue`, as the sample does), alongside
`backup_kind`.
Airflow backup DAGs run each cluster's members in its own pools: `neo4j_<lane>` for the default
cluster (unchanged) and `neo4j_<cluster>_<lane>` for a named one; create those pools.

## Storage

One store, `default`, comes from the environment (`BACKUP_BUCKET`, `CLOUD`, `AWS_REGION`,
`AWS_ENDPOINT_URL_S3`, `S3_SSE`, `S3_SSE_KMS_KEY_ID`, `S3_WRITE_ARGS`) unless `storage:`
defines `default` itself. Name more stores for customers whose data must be kept apart:

| Field | Type | Meaning |
|---|---|---|
| `url` | `s3://<bucket>`, `gs://<bucket>`, `azb://<account>/<container>` | the bucket; key prefixes come from the path layout |
| `region`, `endpoint` | string | for the tool's reads and writes, and (`region`) neo4j-admin's |
| `runner_endpoint` | string | the endpoint as neo4j-admin on the runner sees it, when it differs from `endpoint` (default: `endpoint`) |
| `sse`, `sse_kms_key_id`, `write_args` | string / string / map | encryption and extra arguments on the tool's writes. `write_args` by cloud: S3, any PutObject/CopyObject argument (`BucketKeyEnabled`, `ACL`, ...); Azure, upload keyword arguments such as `encryption_scope` (which copies keep too); GCS, `kms_key_name` (CMEK), `content_type`, `metadata`, `cache_control`, `storage_class`, and any other key is refused when the store is built. `sse`/`sse_kms_key_id` are S3 only |
| `credentials_ref` | secret reference | the store's own S3 key pair, for the tool and neo4j-admin (`s3://` stores only so far) |

A group's backups go to its `storage`, else its cluster's, else `default`. Each op (backup,
verify, aggregate, prune, restore, the MCP status tools) resolves the store from the group. A
cluster's DBMS objects (metadata exports, `system` backups) go to the cluster's store. An
unknown store name fails policy load, naming the group.

neo4j-admin gets a named store's settings: when a backup, aggregate, check or inspect touches
the store, the runner's environment carries its `AWS_REGION`, `AWS_ENDPOINT_URL_S3`
(`runner_endpoint`) and key pair. The Neo4j servers don't: they fetch seeds during a restore with
their own cloud configuration, so every cluster must be able to read each store its groups use
(an IAM role or bucket policy that covers it). `neo4j-backup policy check` lists, writes and
deletes a probe object in every store the policy uses, exits 1 if any fails, and names the stores
the servers must be able to read.

**Secret references** (one syntax for `credentials_ref`, a cluster's `password_ref` and a
build's `env`): `<provider>:<ref>`, for example `env:FINANCE_S3` or
`aws-sm:arn:aws:secretsmanager:eu-west-1:123:secret:finance-s3`, or a bare `<ref>` for the
configured `SECRET_PROVIDER`. An S3 `credentials_ref` resolves to JSON:
`{"access_key_id": "…", "secret_access_key": "…", "session_token": "…"}` (token optional).

## Database names

Each alias-swap restore, load or adopt migration creates a new database. By default it's named
`<slug>-<ts>` (`orders-20261116t120000`). A `naming:` block changes that, for the whole
policy, in a profile, or on one group (the most specific wins):

```yaml
naming:
  physical_template: "prod-{slug}-{ts}"   # default "{slug}-{ts}"
  ts_format: "%Y%m%dt%H%M"                # default NAMING_TS_FMT, else %Y%m%dt%H%M%S
```

Fields: `{alias}` (the alias, lowercased and sanitized), `{slug}`, `{group}`, `{ts}`. The
template must contain `{ts}`, so each restore gets a new name, and `ts_format` can't contain `-`.
When the policy loads, every member's name is rendered with a sample timestamp and checked
against Neo4j's rules (`[a-z0-9.-]`, 3-63 characters, starting and ending with a letter or digit), so a bad
template fails there rather than at a restore. `cleanup` recognises names made from the
template. A `NAMING=module.Class` hook takes precedence over the block. The shell
scripts under `bootstrap/` use the default names only.

Per member, `overrides` can pin the storage folder and the name base:

```yaml
overrides:
  Orders:
    prefix: legacy/orders/   # this member's folder in the store, used as is
    physical_base: orders    # stands in for {slug} and {alias} in its database names
```

`prefix` must end in `/`, can't be a tool folder (`_dbms/`, `_state/`, `_verify/`, `_seed/`
or any other leading `_`), and no two members of one store may share a prefix or nest one
inside another. `physical_base` must give a legal database name with the timestamp.

**Renaming an alias without losing its history.** Backups live under `<group>/<slug>/`, so a
new alias name would start an empty folder. First pin the member to its current folder, then
rename:

1. Add `overrides: {orders: {prefix: shop/orders/}}` and ship the policy (nothing changes).
2. Rename the alias in Neo4j and in the policy, moving the override to the new name:
   `aliases: [orders-v2]`, `overrides: {orders-v2: {prefix: shop/orders/}}`.

Restore, prune, verify and `status` keep reading `shop/orders/`.

**Readable slugs**. By default any alias that isn't already lowercase `[a-z0-9-]` of 3-42
characters gets a hashed slug: `Orders` becomes `orders-cded0933`. `naming.slug_style:
readable` lowercases first and hashes only when sanitizing changed characters (`_`, spaces,
symbols, dots) or the length is out of range, so `Orders` becomes `orders`, in both the
storage folder and new database names. Two members that end up with the same slug fail policy
load, pointing at `overrides.prefix`. The default stays `legacy`.

Switching changes the folder of every mixed-case member, so pin existing members first: for
each one, set `overrides.<alias>.prefix` to its current folder (`neo4j-backup targets` and the
bucket show it, e.g. `shop/orders-cded0933/`), then set `slug_style: readable`. New members get
readable folders; pinned ones keep their history. The shell scripts use the legacy style.

## Field reference

The Status column: `used` fields change what the tool does; `not read` fields are accepted so
older policies still load, and change nothing.

**`db_groups[]`**

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `id` | string | **required** | used | group identifier; part of the storage path + partition key |
| `profile` | string | `null` | used | a name under `profiles:`; its settings apply unless the group sets them |
| `restore_mode` | `alias-swap` \| `by-name` | `alias-swap` | used | `alias-swap` (apps use aliases; restore seeds a new physical + swaps the alias — non-destructive) or `by-name` (no alias; restore targets the database by its own name) |
| `aliases` | list of string | **required** (alias-swap) | used | app-facing Neo4j aliases to back up (one unit each). Use in `alias-swap` mode. |
| `databases` | list of string | **required** (by-name) | used | database **names** to back up/restore directly (must be legal DB names). Use in `by-name` mode instead of `aliases`. |
| `tier` | string | **required** | used | which `tiers` entry sets the schedule (must exist) |
| `retention_days` | int | `7` | used | `prune` deletes backups older than this, whole chains only: per member it keeps the newest chain, any chain with a backup inside the window, and the chain that covers the start of the window (the newest full before it), so a point in time inside the window stays reachable. If the group's tier takes fulls further apart than this, policy load logs a warning and `neo4j-backup targets` lists it under `warnings`: backups are then kept up to the full interval plus `retention_days` |
| `storage` | string | `default` | used | the named store (under `storage:`) for the group's backups, sidecars and restores |
| `s3_prefix` | string | `null` | deprecated | never read; logs a warning. Use `storage` |
| `tenant` | string | `null` | used | a label for selecting a customer's groups: `backup`, `verify`, `aggregate` and `targets` take `--tenant <t>` instead of a group, and MCP read and backup tools take `tenant`. **It is a label, not an access control**: the deployment can reach every database in its clusters whatever the tenant. Settable in a profile. `restore` stays per group |
| `owner` | string | `null` | deprecated | the old name for `tenant`; still read as the tenant, with a warning |
| `overrides[alias].prefix`, `overrides[alias].physical_base` | string | none | used | a member's storage folder, used as is, and the base of its database names; see [Database names](#database-names) |
| `naming` | map | `null` | used | `physical_template`, `ts_format` for new database names, `slug_style: legacy \| readable`; see [Database names](#database-names). Also settable at the top level and in a profile |
| `state` | `active` \| `retired` | `active` | used | offboarding: a retired group has no schedules; its artifacts stay restorable until `retired_on` + `retire_after_days`, then prune deletes them |
| `retired_on` | date | `null` | used | required with `state: retired`: the day the retention period starts |
| `retire_after_days` | int | `retention_days` | used | how long a retired group's artifacts are kept |
| `rpo_minutes` | int | the tier's `rpo_minutes`, else `60` | used | how old a member's newest backup or load may be before it is `stale` in `status` (CLI), `backup_status` (MCP) and the scheduled freshness check (Dagster `status_check`, Airflow `neo4j_status`), which fails its run; policy load warns when the tier's `diff_cron` (or `rebuild_cron`, for a group not backed up by design) runs further apart. A profile can set it |
| `rto_minutes` | int | `120` | used | how long a restore or load waits for a new database to come online (`CREATE DATABASE … WAIT`) before failing, with no alias moved |
| `encryption` | object | `{mode: sse-kms}` | not read | for a group's own key or bucket, use a named store (`storage:` with `sse_kms_key_id`) |
| `topology` | object | `null` | used | cluster shape seeded on restore/import; omit for standalone |
| `overrides` | map | `{}` | used | per alias: `topology` (overrides the group's), `prefix`, `physical_base`, `adopt_from` (the existing database `adopt` puts behind the alias) |
| `load` | object | `{mode: live}` | used | how the group's data arrives and what is kept (below) |

**`db_groups[].load`** chooses the customer's path:

| Customer | `mode` | `writes` | `protect_writes` |
|---|---|---|---|
| Live application | `live` | `read-write` | `true` |
| Bulk loads on an interval only | `rebuild` | `read-only` | n/a |
| Bulk loads, writes allowed but disposable | `rebuild` | `read-write` | `false` |
| Bulk loads, writes kept within a cycle | `rebuild` | `read-write` | `true` |
| Bulk load once, then live | `once` | `read-write` | `true` |
| Static dataset | `once` | `read-only` | n/a |

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `mode` | `live` \| `once` \| `rebuild` | `live` | used | `live`: data written by the application. `once`: loaded once (by hand per IMPORT.md, or by the tool with `build`). `rebuild`: rebuilt in full every `rebuild_cron` |
| `writes` | `read-write` \| `read-only` | `read-write` | used | whether the database accepts writes between loads. `read-only`: every new physical (load, rebuild, restore) gets `ALTER DATABASE … SET ACCESS READ ONLY` before the swap |
| `protect_writes` | bool | `true` for read-write; n/a for read-only | used | back up writes made after a load (full/diff schedules). `false` only for rebuild groups whose writes are disposable. A member is backed up only while its live database is read-write and its writes are protected; the others show `by_design` in the backup result |
| `keep_loads` | int (≥1) | `14` | used | how many load artifacts (dumps in the member's `_seed/` folder) prune keeps per member, newest first; not age-based |
| `baseline` | bool | `true` | used | full backup right after a restore or load |
| `source` | `{storage, prefix}` | the group's store, `<group>/_source/` | used | where rebuild input files live; `storage` names a store |
| `build.kind` | `import` \| `command` | `import` | used | `import`: `neo4j-admin database import full`; `command`: a command or container in the runner |
| `build.input_type` | `csv` \| `parquet` | required for `import` | used | the import's input format |
| `build.args` | list of string | `[]` | used | passed through to the import (the tool puts the database name first) |
| `build.command` / `build.image` / `build.args` | list / string / list | | used | `kind: command`: `command` runs in the runner; `image` runs as its own pod (k8s runner modes; it needs `bash`, `tee` and `base64`). One of the two is required |
| `build.env` | map | `{}` | used | environment for the command; a value `secret:<ref>` is resolved with the secret-reference syntax (see Storage) |
| `build.output` | `import` \| `dump` | `import` | used | `import`: the command writes CSV/Parquet that the built-in import reads (`build.import_args`, paths relative to the work dir, or to `OUTPUT_URI` for an image); `dump`: the command leaves `<work>/<database>.dump` (command only) |
| `build.import_args` | list | `[]` | used | the import's `--nodes` / `--relationships` for `output: import` |
| `build.timeout` | `90m`, `2h`, `3600s` | `2h` | used | the command is stopped after this; the load fails before any seed |
| `notice` | `{hook, before, required}` | `null` | used | warn the customer before a load or rebuild: `hook` (an http(s) URL POSTed JSON, or a command given `NOTICE_*` env vars, as `CUTOVER_HOOK`) gets `group`, `tenant`, `members`, `swap_at`, `loses`, `mode`; the load then waits `before` (e.g. `15m`). A failed hook is logged and the load goes on, unless `required: true`. Settable in a profile |
| `checks` | list of `{query, expect, name?}` | `[]` | used | checks on a new database before its swap: Cypher returning one number; `expect` is `== n`, `>= n`, `<= n` or `between a b`. Run after every load (with the default checks: online, non-zero counts, counts equal to the import's) and by `restore --check` |

Rejected at load, naming the group: `live` with `read-only`; `live` with `protect_writes: false`;
`read-only` with `protect_writes: true`; `rebuild` without `build`, or on a tier without
`rebuild_cron`; `keep_loads` below 1; `import` without `input_type`; `command` with neither
`command` nor `image`; a group that backs up its writes on a tier without `full_cron` and
`diff_cron`; unknown keys.

**`db_groups[].encryption`**

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `mode` | `sse-kms` \| `client-side` \| `none` | `sse-kms` | not read | |
| `kms_key_ref` | string | `null` | not read | use a named store's `sse_kms_key_id` |

**`db_groups[].topology`** — omit entirely for standalone/single-instance (the `TOPOLOGY`
clause is illegal there); set it on a cluster so a restore keeps the intended redundancy
instead of the DBMS default (see [RECOVERY.md](RECOVERY.md), DESIGN.md §3).

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `primaries` | int (≥1) | `1` | used | primary count in the seeded `TOPOLOGY` clause |
| `secondaries` | int (≥0) | `0` | used | secondary count in the seeded `TOPOLOGY` clause |

**`tiers{}`** (named schedules referenced by `db_group.tier`)

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `full_cron` | cron string | required for groups that back up writes | used | when full backups run (the full lane) |
| `diff_cron` | cron string | required for groups that back up writes | used | when differential backups run (the diff lane) |
| `rebuild_cron` | cron string | required for `load.mode: rebuild` groups | used | when rebuilds run (Dagster `<tier>_rebuild`, Airflow `neo4j_rebuild_<tier>`) |
| `verify_cron` | cron string | `0 6 * * *` on tiers with `full_cron` | used | when `verify` runs for the tier's members (Dagster `<tier>_verify`, Airflow `neo4j_verify_<tier>`) |
| `aggregate_cron` | cron string | none | used | when `aggregate` runs (`<tier>_aggregate`, `neo4j_aggregate_<tier>`). Aggregating collapses each chain into one full in place: point-in-time restore between that full and the chain's last diff is gone afterwards, so set it only if you accept that (for example to shorten restore time on very long chains) |
| `prune_cron` | cron string | `0 7 * * 0` on tiers with `full_cron` | used | when the tier's groups are pruned (`<tier>_prune`, `neo4j_prune_<tier>`) |
| `cleanup_cron` | cron string | none | used | when superseded databases are dropped (`<tier>_cleanup`, `neo4j_cleanup_<tier>`) |
| `rpo_minutes` | int | none | used | the default `rpo_minutes` of the tier's groups; a group's or profile's own value wins. Set it to the `diff_cron` interval plus your longest backup plus some slack |
| `cleanup_keep` | int | `1` | used | databases kept before the current one (for `rollback`) |
| `cleanup_older_than_days` | number | `7` | used | the soak period: databases younger than this are kept |

A tier needs at least one of `full_cron`, `diff_cron` and `rebuild_cron`. On a tier that takes
backups, `verify_cron` and `prune_cron` have the defaults above; a rebuild-only tier gets them
only if set.

**`dbms`** (top level; DBMS-wide jobs, run per cluster)

| Field | Type | Default | Status | Meaning |
|---|---|---|---|---|
| `metadata_cron` | cron string | `0 2 * * *` | used | metadata export (Dagster `default_dbms_metadata`, Airflow `neo4j_dbms_metadata_default`) |
| `system_backup_cron` | cron string | `0 1 * * *` | used | `system` database backup (`default_dbms_system`, `neo4j_dbms_system_default`) |
| `keep_metadata` | int (≥1) | `14` | used | metadata exports kept by the DBMS sweep, which runs after each export and system backup |
| `keep_system` | int (≥1) | `14` | used | `system` backups kept |
| `status_cron` | cron string | `*/15 * * * *` | used | freshness check against `rpo_minutes` (Dagster `status_check`, Airflow `neo4j_status`); the run fails when a target is stale. `null` turns it off |

**`schedules`** (top level): `stopped` (default) or `running`, the initial status of the
Dagster schedules and of the `reconcile_registry` sensor. Airflow DAGs follow Airflow's own
pause setting. A tier for load-only groups (read-only, or
`protect_writes: false`) may have only `rebuild_cron`; it gets no backup schedules.

Crons are read in UTC by Dagster's schedules and in Airflow's `default_timezone` (UTC unless
you changed it) by the Airflow DAGs.

## Choosing tiers, retention and rpo

- The `diff_cron` interval is the most a restore can lose. Choose it per customer: hourly for
  an application whose writes can't be redone, daily for data that can be reloaded.
- `full_cron` sets how long a chain grows. A restore replays the full and every differential
  after it, so weekly fulls with hourly diffs mean up to 168 diffs to replay. Daily fulls keep
  restores quick; weekly fulls save storage.
- `retention_days` is how far back a customer can restore. Prune keeps whole chains, so
  storage is roughly `retention_days` plus one full interval of backups.
- `rpo_minutes` is when the freshness check pages you. Set it on the tier to the diff interval
  plus the longest backup plus some slack (gold, hourly diffs: 90; daily diffs: 1500). Too
  small and it pages on every schedule; policy load warns when it is smaller than the diff
  interval.
- Start new customers on an existing tier; add a tier only for a schedule nobody has.

## What you usually set

Per group: `id`, `aliases`, `tier` (or a `profile`), `retention_days`, and on a cluster
`topology`. Per tier: `full_cron`, `diff_cron` (their gap is how much a restore can lose),
`rpo_minutes`, and `cleanup_cron` once restores are routine. A group that is loaded rather than written
also needs `load:`. Everything else has a working default.
