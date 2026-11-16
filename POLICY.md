# Policy

The policy is the foundation of this tool: one YAML file that declares **what to back
up, on what schedule, and how long to keep it**. Everything else (env vars, the runner,
the schedules) just serves the policy. It is the one file you must author.

- Loaded from the path in the `NEO4J_BACKUP_POLICY` env var (default `policies/demo.yaml`).
- Schema (authoritative): `orchestrator/neo4j_backup_dagster/policy.py`.
- Validated on load: aliases must be legal Neo4j aliases, and every group's `tier` must
  exist under `tiers`.

## A complete policy

Copy this, delete what you don't need, and edit it. Every field is shown. Comments mark
**[used]** (the pipeline acts on it) vs **[decl]** (declarative — accepted and validated,
but not wired to behavior yet).

```yaml
# Two top-level keys: db_groups (a list) and tiers (a map).

db_groups:
  # ----- one entry per backup group (the policy + PITR-alignment unit) -----
  - id: acme                        # [used] group id; part of storage path + partition key
    aliases:                        # [used] app-facing Neo4j aliases to back up (one unit each)
      - acme-orders
      - acme-graph
      - acme-audit
    tier: gold                      # [used] must match a key under `tiers:` below
    retention_days: 30              # [used] prune deletes whole chains older than this (see below)

    owner: acme-corp                # [decl] reporting / ownership tag only
    s3_prefix: s3://backups/acme/   # [decl] not read yet; bucket comes from BACKUP_BUCKET env
    rpo_minutes: 60                 # [decl] target, not enforced
    rto_minutes: 120                # [decl] target, not enforced
    encryption:                     # [decl] recorded intent; SSE-KMS is applied by your bucket
      mode: sse-kms                 #        one of: sse-kms | client-side | none
      kms_key_ref: acme-key         #        KMS key id/alias for this group
    topology:                       # [used] cluster shape a restore seeds into (omit on standalone)
      primaries: 3                  #        CREATE DATABASE … TOPOLOGY 3 PRIMARIES 0 SECONDARIES
      secondaries: 0
    overrides: {}                   # [decl] per-alias cadence overrides; not wired
                                    #        (may carry `topology:` per alias — that IS wired)
    load:                           # [decl] how data arrives and what is kept (omit: live)
      mode: live                    #        live | once | rebuild
      writes: read-write            #        read-write | read-only
      protect_writes: true          #        back up writes made after a load
      keep_loads: 14                #        load artifacts kept restorable
      baseline: true                # [used] full backup right after a restore (#97)

  # ----- a group rebuilt from source files every night, read-only in between -----
  - id: catalog
    aliases: [catalog]
    tier: nightly
    s3_prefix: s3://backups/catalog/
    load:
      mode: rebuild
      writes: read-only
      keep_loads: 14
      source: { storage: default, prefix: catalog/_source/ }   # default: <group>/_source/
      build:
        kind: import                # import | command (a command or container in the runner)
        input_type: parquet         # csv | parquet (import only)
        args: [--nodes=Product=products/*.parquet, --relationships=IN=in/*.parquet]

  # ----- a second group on a lighter tier (minimal: only the [used] fields + s3_prefix) -----
  - id: globex
    aliases: [globex-main]
    tier: bronze
    retention_days: 7
    s3_prefix: s3://backups/globex/

# ----- named schedules referenced by db_group.tier -----
tiers:
  gold:                             # [used]
    full_cron: "0 2 * * *"          #   daily full at 02:00
    diff_cron: "0 * * * *"          #   hourly differential
  bronze:
    full_cron: "0 4 * * 0"          #   weekly full (Sundays 04:00)
    diff_cron: "0 0 * * *"          #   daily differential
  nightly:                          # a tier for load-only groups may have only rebuild_cron
    rebuild_cron: "0 1 * * *"       # [decl] when rebuilds run (#108)
```

The smallest valid group needs only `id`, `aliases`, `tier`, `retention_days`, and
`s3_prefix` (the last is required by the schema even though it's declarative today).

## Field reference

**`db_groups[]`**

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `id` | string | **required** | used | group identifier; part of the storage path + partition key |
| `restore_mode` | `alias-swap` \| `by-name` | `alias-swap` | used | `alias-swap` (apps use aliases; restore seeds a new physical + swaps the alias — non-destructive) or `by-name` (#48: no alias; restore targets the database by its own name) |
| `aliases` | list of string | **required** (alias-swap) | used | app-facing Neo4j aliases to back up (one unit each). Use in `alias-swap` mode. |
| `databases` | list of string | **required** (by-name) | used | database **names** to back up/restore directly (must be legal DB names). Use in `by-name` mode instead of `aliases`. |
| `tier` | string | **required** | used | which `tiers` entry sets the schedule (must exist) |
| `retention_days` | int | `7` | used | `prune` deletes backups older than this, whole chains only: per member it keeps the newest chain, any chain with a backup inside the window, and the chain that covers the start of the window (the newest full before it), so a point in time inside the window stays reachable. If the group's tier takes fulls further apart than this, policy load logs a warning and `neo4j-backup targets` lists it under `warnings`: backups are then kept up to the full interval plus `retention_days` |
| `s3_prefix` | string | **required** | decl | not read; the bucket comes from `BACKUP_BUCKET` |
| `owner` | string | `null` | decl | reporting / ownership tag |
| `rpo_minutes` | int | `60` | decl | target, not enforced |
| `rto_minutes` | int | `120` | decl | target, not enforced |
| `encryption` | object | `{mode: sse-kms}` | decl | recorded intent; SSE-KMS applied by your bucket |
| `topology` | object | `null` | used | cluster shape seeded on restore/import; omit for standalone |
| `overrides` | map | `{}` | decl | per-alias cadence overrides; not wired (but `overrides[alias].topology` **is** — it overrides the group topology for that alias) |
| `load` | object | `{mode: live}` | decl (see below) | how the group's data arrives and what is kept (#103) |

**`db_groups[].load`** — chooses the customer's path. Validation runs now; loads and rebuilds
arrive with #107 and #108, write enforcement with #112. `baseline` is used today (#97).

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
| `mode` | `live` \| `once` \| `rebuild` | `live` | decl | `live`: data written by the application. `once`: loaded once (by hand per IMPORT.md, or by the tool with `build`). `rebuild`: rebuilt in full every `rebuild_cron` |
| `writes` | `read-write` \| `read-only` | `read-write` | decl | whether the database accepts writes between loads |
| `protect_writes` | bool | `true` for read-write; n/a for read-only | decl | back up writes made after a load (full/diff schedules). `false` only for rebuild groups whose writes are disposable |
| `keep_loads` | int (≥1) | `14` | decl | how many load artifacts stay restorable |
| `baseline` | bool | `true` | used | full backup right after a restore (and, later, a load) |
| `source` | `{storage, prefix}` | the group's store, `<group>/_source/` | decl | where rebuild input files live; `storage` names a store (#105) |
| `build.kind` | `import` \| `command` | `import` | decl | `import`: `neo4j-admin database import full`; `command`: a command or container in the runner (#109) |
| `build.input_type` | `csv` \| `parquet` | required for `import` | decl | the import's input format |
| `build.args` | list of string | `[]` | decl | passed through to the import (the tool puts the database name first) |
| `build.command` / `build.image` / `build.env` | list / string / map | | decl | for `kind: command` (#109): one of `command` or `image` is required |

Rejected at load, naming the group: `live` with `read-only`; `live` with `protect_writes: false`;
`read-only` with `protect_writes: true`; `rebuild` without `build`, or on a tier without
`rebuild_cron`; `keep_loads` below 1; `import` without `input_type`; `command` with neither
`command` nor `image`; a group that backs up its writes on a tier without `full_cron` and
`diff_cron`; unknown keys.

**`db_groups[].encryption`**

| Field | Type | Required / default | Status | Meaning |
|---|---|---|---|---|
| `mode` | `sse-kms` \| `client-side` \| `none` | `sse-kms` | decl | intended at-rest encryption mode |
| `kms_key_ref` | string | `null` | decl | KMS key id/alias for the group |

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
| `rebuild_cron` | cron string | required for `load.mode: rebuild` groups | decl | when rebuilds run (#108) |

A tier needs at least one of the three. A tier for load-only groups (read-only, or
`protect_writes: false`) may have only `rebuild_cron`; it gets no backup schedules.

## What you actually tune

The **used** fields: `id`, `aliases`, `tier`, `retention_days`, `topology` (on clusters),
and the `tiers` crons.
The **decl** fields are accepted and validated but don't change behavior yet — leave them
at sane values. (`s3_prefix` would become live if per-group buckets are wired.)
