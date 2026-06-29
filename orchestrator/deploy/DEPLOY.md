# Adapting to your environment

This project validates the **logic** locally; production deployment is intentionally
left to teams to adapt. This file is the short list of what to wire, not a step-by-step
for any one platform. The execution model used in every local smoke
(`PipesSubprocessClient`) is already the VM model — k8s is optional.

## Runner requirements (any platform)

The component that runs `neo4j-admin` (backup / aggregate / verify) needs:

1. The `neo4j-admin` binary — installed on the worker, or run the Neo4j image.
2. Network to the Neo4j backup port (6362) and the object store.
3. A **scratch volume** sized to the largest full backup (multi-TB), at `--temp-path`,
   separate from the orchestrator's run storage.
4. Memory caps: `HEAP_SIZE` + `--pagecache` (DESIGN.md §5.5).
5. Object-store + KMS credentials (instance role / IRSA / static keys).

Restore needs none of these — it is pure Cypher over Bolt from the Dagster worker.

## Two execution modes

- **Subprocess (default, VM/EC2/ECS)** — `RunnerResource.exec_prefix = []` with
  `neo4j-admin` on the worker (or a `docker run` prefix). This is the validated local
  path; scratch is an attached volume, concurrency is bounded by the lanes below.
- **Kubernetes (optional)** — set `RUNNER_MODE=k8s` + `RUNNER_IMAGE` (a Neo4j image with
  `neo4j-admin`). Each backup then runs in its own pod via `PipesK8sClient` with a fresh
  ephemeral scratch PVC. Also reads `RUNNER_NODE_SELECTOR` (JSON), `RUNNER_MEMORY_LIMIT`,
  `RUNNER_SCRATCH_STORAGE`, `RUNNER_SERVICE_ACCOUNT` (IRSA for S3/KMS). It's built in (no
  code change) but authored against the API — validate the pod spec on your cluster.

## Code location drop-in

One isolated code location (DESIGN.md §6.8):

```yaml
# workspace.yaml (OSS)
load_from:
  - python_module: { module_name: neo4j_backup_dagster.definitions,
                     executable_path: /path/to/.venv/bin/python }
# dagster_cloud.yaml (Dagster+): a locations: entry with its own image
```

Pin the location's `dagster` close to the host. The one shared-instance change is the
lane limits in [`dagster.yaml`](dagster.yaml) — coordinate with the instance owner.

## Concurrency lanes + source protection

Apply [`dagster.yaml`](dagster.yaml): `backup_kind: full|diff` run limits (full bounded
by scratch capacity) + `pool="neo4j"` on the backup asset to protect the source member.

## Object store, encryption, IAM (your structure)

The pipeline is object-store-agnostic: it takes a configurable bucket and writes
`<group>/<slug>/<physical>/` prefixes. How you map groups to **buckets, KMS keys, and
IAM** is yours — the project does not impose it. SSE-KMS is server-side at the store
(transparent to the pipeline); to get a per-group key, the simplest path is a bucket per
group (S3 default encryption is bucket-wide), but a shared bucket + one key is fine. The
policy carries `s3_prefix` and `kms_key_ref` per group so you can wire per-group
buckets/keys if you want. Grant the DB nodes `kms:Decrypt` so seed-from-URI restore
reads encrypted artifacts. Locally one MinIO bucket + one key stands in.
