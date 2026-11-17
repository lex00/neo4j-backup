# Policy fields

Every field the policy accepts. Examples and advice are in [POLICY.md](POLICY.md).

Top level: `db_groups` and `tiers` are required. The other sections are optional.
`schedules: running` starts the Dagster schedules on (default `stopped`). Airflow uses its own pause setting.
Crons are UTC in Dagster and Airflow's `default_timezone` in Airflow.

## db_groups[]

| Field | Default | Meaning |
|---|---|---|
| `id` | required | part of the storage path |
| `tier` | required | a key under `tiers:` |
| `aliases` | required | one member per alias |
| `restore_mode` | `alias-swap` | `by-name`: no alias. List `databases` instead of `aliases` |
| `profile`, `cluster`, `storage` | none | names under `profiles:`, `clusters:`, `storage:` |
| `retention_days` | `7` | prune deletes whole chains older than this |
| `rpo_minutes` | the tier's, else `60` | the newest backup's age before the member is `stale` |
| `rto_minutes` | `120` | how long a restore or load waits for the new database |
| `topology` | none | `primaries` (1) and `secondaries` (0). Omit on standalone |
| `overrides` | `{}` | per alias: `topology`, `prefix`, `physical_base`, `adopt_from` |
| `naming` | none | `physical_template`, `ts_format`, `slug_style` |
| `tenant` | none | a label for `--tenant`. It is not access control |
| `load` | `{mode: live}` | how data arrives (below) |
| `state` | `active` | `retired` removes the group's schedules |
| `retired_on` | | required when retired: the day retention starts |
| `retire_after_days` | `retention_days` | then prune deletes a retired group's artifacts |
| `encryption`, `s3_prefix` | | not read. Use a store's `sse_kms_key_id` |
| `owner` | | old name for `tenant`. Logs a warning |

Prune keeps each member's newest chain and the chains that cover the window.
The policy load warns when the tier's fulls are further apart than `retention_days`.
An override `prefix` must end in `/`, can't start with `_`, and can't overlap another member's.

## db_groups[].load

| Customer | `mode` | `writes` | `protect_writes` |
|---|---|---|---|
| Live application | `live` | `read-write` | `true` |
| Bulk loads on an interval only | `rebuild` | `read-only` | n/a |
| Bulk loads, writes disposable | `rebuild` | `read-write` | `false` |
| Bulk loads, writes kept within a cycle | `rebuild` | `read-write` | `true` |
| Bulk load once, then live | `once` | `read-write` | `true` |
| Static dataset | `once` | `read-only` | n/a |

| Field | Default | Meaning |
|---|---|---|
| `mode` | `live` | `live`, `once`, or `rebuild` (every `rebuild_cron`) |
| `writes` | `read-write` | `read-only` sets each new database read-only before the swap |
| `protect_writes` | `true` if read-write | back up writes made after a load |
| `keep_loads` | `14` | load dumps kept per member |
| `baseline` | `true` | full backup right after a restore or load |
| `source` | the group's store, `<group>/_source/` | `{storage, prefix}` of rebuild input files |
| `build` | none | required for `rebuild` (below) |
| `checks` | `[]` | `{query, expect, name}`: Cypher returning one number, checked before the swap |
| `notice` | none | `{hook, before, required}`: warn the customer before a load |

`expect` is `== n`, `>= n`, `<= n` or `between a b`. Checks also run on `restore --check`.

`notice.hook` is an http(s) URL (POSTed JSON) or a command (`NOTICE_*` variables).
The load waits `before` (default `0m`) after it. A failed hook stops the load only with `required: true`.

### build

| Field | Default | Meaning |
|---|---|---|
| `kind` | `import` | `import` runs `neo4j-admin database import full`. `command` runs your code |
| `input_type` | | `csv` or `parquet`. Required for `import` |
| `args` | `[]` | passed through. An import gets the database name first |
| `command` | `[]` | runs in the runner |
| `image` | none | runs as its own pod. It needs `bash`, `tee` and `base64` |
| `env` | `{}` | a value `secret:<ref>` is resolved |
| `output` | `import` | `dump`: the command leaves `<work>/<database>.dump` |
| `import_args` | `[]` | `--nodes` and `--relationships` for `output: import` |
| `timeout` | `2h` | for example `90m` or `3600s` |

The load fails, naming the group, on any of these:

- `live` with `read-only` or with `protect_writes: false`
- `read-only` with `protect_writes: true`
- `rebuild` without `build`, or on a tier without `rebuild_cron`
- `import` without `input_type`
- `command` with neither `command` nor `image`, or with `output: import` and no `import_args`
- a group that backs up writes on a tier without `full_cron` and `diff_cron`

## tiers{}

A tier needs at least one of `full_cron`, `diff_cron` and `rebuild_cron`.

| Field | Default | Meaning |
|---|---|---|
| `full_cron`, `diff_cron` | | backups. Required for groups that back up writes |
| `rebuild_cron` | | required for `rebuild` groups |
| `rpo_minutes` | | default for the tier's groups |
| `verify_cron` | `0 6 * * *` with `full_cron` | `verify` |
| `prune_cron` | `0 7 * * 0` with `full_cron` | `prune` |
| `aggregate_cron` | none | `aggregate`. It ends point-in-time restore inside each chain |
| `cleanup_cron` | none | drops superseded databases |
| `cleanup_keep` | `1` | databases kept before the current one |
| `cleanup_older_than_days` | `7` | younger databases are kept |

Jobs are `<tier>_<job>` in Dagster and `neo4j_<job>_<tier>` in Airflow.

## dbms

| Field | Default | Meaning |
|---|---|---|
| `metadata_cron` | `0 2 * * *` | metadata export |
| `system_backup_cron` | `0 1 * * *` | `system` database backup |
| `keep_metadata`, `keep_system` | `14` | how many of each are kept |
| `status_cron` | `*/15 * * * *` | the freshness check. `null` turns it off |

## clusters{}

| Field | Default | Meaning |
|---|---|---|
| `bolt_uri` | required | |
| `user` | `neo4j` | |
| `password_ref` | `SECRET_PROVIDER`'s default | a secret reference |
| `backup_source` | `neo4j:6362` | one address or a list |
| `storage` | `default` | the cluster's store |
| `topology` | none | default for its groups |
| `cypher_version` | none | `"5"` on a Cypher 5 cluster |
| `cutover` | none | `strategy: alias-swap`, or `external` with a `hook` |
| `backup_upload` | none | `admin` or `pipeline`, as `BACKUP_UPLOAD` |

## storage{}

| Field | Meaning |
|---|---|
| `url` | `s3://<bucket>`, `gs://<bucket>` or `azb://<account>/<container>` |
| `region`, `endpoint` | for the tool. neo4j-admin also gets `region` |
| `runner_endpoint` | the endpoint as neo4j-admin sees it (default: `endpoint`) |
| `sse`, `sse_kms_key_id` | S3 encryption on the tool's writes |
| `write_args` | extra write arguments (below) |
| `credentials_ref` | the store's own S3 key pair, for the tool and neo4j-admin |

`write_args`: on S3, PutObject and CopyObject arguments. On Azure, upload arguments such as `encryption_scope`.
On GCS, only these: `kms_key_name` `content_type` `metadata` `cache_control` `storage_class`.
