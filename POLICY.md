# Policy

The policy is a YAML file that lists the database groups, their schedule tiers and their retention.
Terms (group, member, alias, tier and seed): [GLOSSARY.md](GLOSSARY.md).

- Loaded from `NEO4J_BACKUP_POLICY` (a path or `s3://bucket/key.yaml`).
- Every field: [POLICY-FIELDS.md](POLICY-FIELDS.md). Schema: `orchestrator/neo4j_backup_core/policy.py`.
- Unknown keys, tiers or stores and bad crons or names fail the load.
  `neo4j-backup policy validate <file>` checks a file before shipping.

## Policy or environment

The policy holds per-customer settings. The environment holds this installation's connections.

| In the policy | In the environment ([ENVIRONMENT.md](ENVIRONMENT.md)) |
|---|---|
| groups, tiers, profiles, `load:` | `NEO4J_BACKUP_POLICY`, `POLICY_CACHE_TTL`, `POLICY_LOADER` |
| named `clusters:` and `storage:` | the `default` cluster and store, when the policy omits them |
| secret references | the secrets, `SECRET_PROVIDER` and cloud credentials |
| `naming:`, `overrides`, `dbms:`, `schedules:` | runner settings, `BACKUP_UPLOAD` and hooks (`NAMING`, `CUTOVER_STRATEGY`) |

## A complete policy

Copy this and trim it.

```yaml
storage:                            # named stores; omit to use the environment's one store
  finance:
    url: s3://neo4j-backups-finance # or gs://<bucket>, azb://<account>/<container>
    sse_kms_key_id: alias/finance-backups

db_groups:
  - id: acme                        # part of the storage path
    aliases: [acme-orders, acme-graph, acme-audit]   # restored together, to one instant
    tier: gold                      # a key under `tiers:`
    retention_days: 30
    tenant: acme-corp
    storage: finance
    topology: { primaries: 3, secondaries: 0 }   # omit on a standalone server
    load: { mode: live, writes: read-write, protect_writes: true }

  # rebuilt from source files every night, read-only in between
  - id: catalog
    aliases: [catalog]
    tier: rebuild_nightly
    load:
      mode: rebuild
      writes: read-only
      keep_loads: 14
      source: { storage: default, prefix: catalog/_source/ }   # default: <group>/_source/
      build:
        kind: import                # import | command
        input_type: parquet         # csv | parquet
        args: [--nodes=Product=products/*.parquet, --relationships=IN=in/*.parquet]

  # the minimum: id, aliases, tier
  - id: globex
    aliases: [globex-main]
    tier: bronze

tiers:
  gold:
    full_cron: "0 2 * * *"          # daily full at 02:00 UTC
    diff_cron: "0 * * * *"          # hourly differential
    rpo_minutes: 90
  bronze:
    full_cron: "0 4 * * 0"          # weekly full, Sundays
    diff_cron: "0 0 * * *"          # daily differential
    rpo_minutes: 1500
  rebuild_nightly:                  # load-only groups: no backups, no point-in-time restore
    rebuild_cron: "0 1 * * *"
    rpo_minutes: 1500
```

## Profiles

A profile names a set of group settings. A group uses one with `profile:` and sets only what differs.
`policies/demo.yaml` has these four:

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
- The group's own values win. `load:` merges key by key. Other fields are replaced, lists included.
- A profile can supply `tier`. An unknown profile or profile field fails the load.
- `neo4j-backup policy show <group>` prints the effective settings and where each came from.

## Clusters

The `default` cluster comes from the environment (`NEO4J_BOLT_URI`, `NEO4J_BACKUP_SOURCE`, ...).
A `default` entry under `clusters:` replaces it. A group picks a cluster with `cluster:`.

```yaml
clusters:
  us-east:
    bolt_uri: neo4j://neo4j-use1.internal:7687
    password_ref: aws-sm:neo4j/use1
    backup_source: [use1-a:6362, use1-b:6362]
db_groups:
  - id: orders
    cluster: us-east
    aliases: [orders]
    tier: gold
```

- Group ids are unique. An alias belongs to one group per cluster.
- The metadata export and the `system` backup run per cluster on the `dbms:` crons.
  A named cluster writes them under `_dbms/<cluster>/`.
- Dagster runs carry a `neo4j_cluster` tag. Limit it in `dagster.yaml` with `applyLimitPerUniqueValue`.
- Airflow uses the pools `neo4j_<cluster>_<lane>` for a named cluster. Create them.

## Storage

The `default` store comes from the environment (`BACKUP_BUCKET`, `CLOUD`). A `default` entry under `storage:` replaces it.
Name more stores to keep customers' data apart.
A group's backups go to its `storage`, else its cluster's, else `default`.

The Neo4j servers fetch seeds with their own cloud configuration and ignore the store's settings.
Every cluster must be able to read each store its groups use.
`neo4j-backup policy check` writes a probe object to every store and exits 1 if one fails.

Secret references (`credentials_ref`, `password_ref`, a build's `env`) look like `env:FINANCE_S3`
or `aws-sm:<arn>`. A bare `<ref>` uses `SECRET_PROVIDER`. An S3 `credentials_ref` resolves to JSON
with `access_key_id`, `secret_access_key` and an optional `session_token`.

## Database names

A restore, load or adopt creates a new database named `<slug>-<ts>` (`orders-20261116t120000`).
A `naming:` block at the top level, in a profile or on a group changes that:

```yaml
naming:
  physical_template: "prod-{slug}-{ts}"   # also {alias}, {group}; {ts} is required
  ts_format: "%Y%m%dt%H%M"                # no "-"
  slug_style: readable                    # Orders -> orders, not orders-cded0933
```

Names are checked against Neo4j's rules when the policy loads. A `NAMING` hook wins over the block.

Per member, `overrides` pins the storage folder and the name base:

```yaml
overrides:
  Orders:
    prefix: legacy/orders/   # this member's folder in the store, used as is
    physical_base: orders    # stands in for {slug} and {alias} in its database names
```

Pin a member's folder before renaming its alias or switching `slug_style`. Otherwise it loses its history.

1. Add `overrides: {orders: {prefix: shop/orders/}}` and ship the policy. `neo4j-backup targets` shows the folder.
2. Rename the alias in Neo4j and in the policy, and move the override to the new name.

## Choosing tiers, retention and rpo

- The `diff_cron` interval is the most a restore can lose. Use hourly for live writes, daily for reloadable data.
- A restore replays the full and every diff after it. Weekly fulls with hourly diffs mean up to 168 diffs.
- `retention_days` is how far back a customer can restore. Storage is about that plus one full interval.
- `rpo_minutes` sets when the freshness check pages. Use the diff interval plus the longest backup and some slack.

## Usual settings

Per group: `id`, `aliases`, `tier` (or a `profile`) and `retention_days`. Add `topology` on a cluster.
A group that is loaded rather than written also needs `load:`.
Per tier: `full_cron`, `diff_cron`, `rpo_minutes`, and `cleanup_cron` once restores are routine.
