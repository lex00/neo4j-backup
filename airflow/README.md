# Airflow adapter

Airflow 3.x DAGs over `neo4j_backup_core` — the same backup/restore engine the
[Dagster adapter](../orchestrator/README.md) drives, exposed as Airflow DAGs instead of a
Dagster code location. **Pick one**, Dagster *or* Airflow: both shell out to standard
`neo4j-admin` and run standard Cypher, both read the same [policy](../POLICY.md) and write
ordinary `.backup` files. Same **just tooling, no lock-in** story — remove it and you still
have standard Neo4j backups.

The logic lives once in `neo4j_backup_core` (naming, policy, Bolt client, object store,
runner command-building + pod spec); each adapter is a thin binding. Architecture:
[`../DESIGN.md`](../DESIGN.md) §6.

## What's here

| Path | Role |
|---|---|
| `dags/backup_dag.py` | One backup DAG per **(tier, lane)** — `neo4j_backup_<tier>_<full\|diff>`. Fans out over the tier's `(group, alias)` via dynamic task mapping. |
| `dags/restore_dag.py` | `neo4j_restore` — manual, params `group_id` + `restore_until`. Group-aligned seed-from-URI then alias swap (pure Cypher). |
| `dags/avp_dags.py` | `neo4j_aggregate`, `neo4j_verify` (non-destructive), `neo4j_prune` — retention / consistency. |
| `dags/metadata_dag.py` | `neo4j_metadata_backup` / `neo4j_metadata_restore` — agentless DBMS metadata export/replay (#14). |
| `dags/scaffold_dag.py` | Trivial policy-loading DAG (smoke of the wiring). |
| `neo4j_backup_airflow/config.py` | Env → core clients (the [Dagster env table](../orchestrator/README.md#environment-variables), verbatim). |
| `neo4j_backup_airflow/execution.py` | `run_admin` — subprocess or KubernetesPodOperator per `RUNNER_MODE`. |

## The DAG set

| DAG(s) | Schedule | What it does |
|---|---|---|
| `neo4j_backup_<tier>_full` | tier's `full_cron` | `neo4j-admin database backup --type FULL` of each alias's live physical, into its per-store prefix. |
| `neo4j_backup_<tier>_diff` | tier's `diff_cron` | same, `--type DIFF` — extends the chain in that physical's prefix. |
| `neo4j_restore` | manual (`-c '{"group_id":"demo"}'`) | seed a fresh physical per alias from the latest artifact (optionally `restore_until` for PITR), then swap each alias. |
| `neo4j_aggregate` | weekly | collapse each chain into one recovered full (in place). |
| `neo4j_verify` | daily | **non-destructive** consistency check — copy chain to a scratch prefix, aggregate + `neo4j-admin database check`, then delete the copy. |
| `neo4j_prune` | weekly | age-based retention; always keep the chain head. |
| `neo4j_metadata_backup` | daily | export users/roles/privileges/aliases as replayable Cypher to `_dbms/` (pure Bolt). |
| `neo4j_metadata_restore` | manual | replay the latest (or a given `key`) against `system` over Bolt. |

One backup DAG is generated per tier × lane from the policy at parse time, so adding a tier
or changing a cron is a policy edit — no DAG code changes.

## How it maps to the Dagster adapter

| Concept | Dagster | Airflow |
|---|---|---|
| Per-(group, alias) fan-out | dynamic partitions | dynamic task mapping (`.expand()`) |
| Full/diff concurrency lanes | `tag_concurrency_limits` | **pools** `neo4j_full` (1) / `neo4j_diff` (N) |
| In-process validation | `execute_in_process` | `dag.test()` |
| `neo4j-admin` execution | Pipes (subprocess / `PipesK8sClient`) | `run_admin` (subprocess / `KubernetesPodOperator`) |
| Restore | `restore_group` job | `neo4j_restore` DAG |

Two Airflow shapes worth knowing: the restore DAG maps `seed` over a **separate** `aliases`
task (Airflow can `.expand()` only over a whole task's output, not a sub-key of one), and
the alias `swap` is a single barrier task downstream of all seeds.

## Configuration

Same four surfaces and the **same environment variables** as the Dagster adapter —
see the [Orchestrator config section](../orchestrator/README.md#configuration-what-you-edit-and-where)
and [env table](../orchestrator/README.md#environment-variables). The only Airflow-specific
difference is concurrency: lanes are Airflow **pools**, set once per environment —

```bash
airflow pools set neo4j_full 1   "serialize full backups"
airflow pools set neo4j_diff 6   "diff backup parallelism"
```

(`just airflow-standalone` and the smokes create these for you.) The policy, recovery
model, and per-group encryption are identical and documented once on the
[Policy](../POLICY.md) and [Recovery](../RECOVERY.md) pages.

## Execution modes

`neo4j-admin` (backup / aggregate / verify) runs via `neo4j_backup_airflow.execution.run_admin`:

- `RUNNER_MODE=subprocess` (default, validated) — runs on the Airflow worker, which needs
  `neo4j-admin`, the scratch volume, network to 6362, and S3/KMS access. (Locally the
  smokes set `RUNNER_EXEC_PREFIX` to run it inside the demo container.)
- `RUNNER_MODE=k8s` — each command runs in its own `KubernetesPodOperator` pod with a fresh
  ephemeral scratch PVC, built from the same core `BackupRunner` fields the Dagster adapter
  feeds `PipesK8sClient` (`RUNNER_IMAGE` + the k8s vars in the env table). Validated against
  k3d by `smoke_k8s.py`.

Restore is always pure Cypher over Bolt — no runner needed.

## Install (uv, Python 3.13)

Airflow needs its constraints file; it goes in its own venv, isolated from Dagster:

```bash
just airflow-install          # uv venv + pinned Airflow + adapter (no deps) + neo4j + pytest
```

Override the pins with `AIRFLOW_VERSION=… PYTHON_VERSION=… just airflow-install`. See
`airflow/install.sh` for the underlying steps.

## Run it locally

Against the Compose stack (`just up && just bootstrap`):

```bash
just airflow-smoke        # backup → verify → restore → prune (dag.test, in-process)
just airflow-pitr         # real full+diff chain + point-in-time restore (seedRestoreUntil)
just airflow-k8s-smoke    # KubernetesPodOperator mode against k3d (needs `just k3d-up`)
just airflow-standalone   # scheduler + UI on :8080, DAGs wired to the stack
```

`dag.test()` runs a DAG in-process against the live stack — the Airflow analogue of
Dagster's `execute_in_process`, and how the smokes validate without a running scheduler.
The smokes live in `airflow/smoke_e2e.py`, `smoke_pitr.py`, `smoke_k8s.py`.

## Validate DAGs parse (no stack needed)

```bash
AIRFLOW_HOME=/tmp/af NEO4J_BACKUP_POLICY=policies/demo.yaml \
  airflow/.venv/bin/python -m pytest airflow/tests/
```

This is the same DagBag import-error check CI runs.

## Choosing Dagster or Airflow

They are interchangeable over the same core and policy. Pick the one you already run.
Dagster gives asset lineage and the partition UI out of the box; Airflow gives pools,
the operator ecosystem, and dynamic task mapping. Migrating later means re-pointing the
same env at the other adapter — the artifacts, policy, and naming don't change.
