# Local Dev Stack

One Neo4j Enterprise server, the floci S3 and KMS emulator and a runner with `neo4j-admin`,
driven by `just`. It needs Docker with Docker Compose.
[Getting started](GETTING-STARTED.md) walks through it.

## Recipes

| Recipe | Does |
|---|---|
| `up` / `down` | start or tear down the stack (`down` removes volumes) |
| `bootstrap` | create the demo group's databases and load demo data |
| `fresh` | `up` + `bootstrap` |
| `try` | reset the stack, then back up, delete data and restore to before the delete, with a summary per step |
| `backup [group]` | online backup of each database in the group to the bucket |
| `restore [group] [until]` | seed a new database per alias from its newest backup (or to `until`), then swap aliases |
| `demo-pitr` | build a full, change, differential chain and restore to before the change |
| `artifacts [prefix]` | list backup files in the bucket |
| `logs` / `ps` / `urls` | observe the stack |
| `cli-smoke` / `airflow-smoke` / `mcp-smoke` | run the loop through the CLI, Airflow, or the [MCP server](MCP.md) |

`mcp-smoke` needs the `[mcp]` extra. Dagster's smoke is `orchestrator/smoke_local.py`.

## Layout

Two networks model the real boundary: `neo4j` for database servers, `ops` for the bucket. The runner
bridges them for backups: it pulls over port 6362 and writes to the bucket. The Neo4j server is
also on `ops` with S3 credentials, because a restore has the server fetch the seed itself. In a
real cluster every server needs that bucket access.

The demo group (`policies/demo.yaml`, tier gold) has three databases that share ids.
They are `acme-orders`, `acme-graph` and `acme-audit` (an event log). A point-in-time restore brings all
three back to one instant.

## The bucket: floci

[floci](https://hub.docker.com/r/floci/floci) (`floci/floci:1.5.34`) emulates S3 and KMS and accepts
any credentials. The endpoint is `http://s3:4566` inside the compose network and
`http://localhost:9000` on the host.

`docker/create-bucket.sh` creates the `neo4j-backups` bucket. Its default encryption is the KMS key
`alias/demo-group-key`. floci records the default but doesn't apply it, so
`neo4j-admin`'s direct writes are unencrypted locally. To see encrypted writes, run with
`BACKUP_UPLOAD=pipeline` and `S3_SSE=aws:kms`. Real S3 applies the default to every write.

## A three-server cluster

```bash
docker compose --env-file .env -f docker/compose.yaml -f docker/compose.cluster.yaml \
  --profile cluster up -d --wait core1 core2 core3
just cluster-smoke
```

Host clients use `neo4j://localhost:7691` (Bolt on 7691-7693). The runner reaches the backup port
as `core1:6362` to `core3:6362`. `just cluster-smoke` backs up a database with 1 primary and 2
secondaries. It restores it with that topology and checks every server sees the alias move.
`just down` removes these containers too.

## A second cluster

```bash
docker compose --env-file .env -f docker/compose.yaml --profile multi up -d --wait neo4j-b
```

This starts an independent Neo4j (Bolt on host port 7688, backup at `neo4j-b:6362`) that reads the
same bucket. `just move-smoke` uses it as cluster `west` to test `restore --to-cluster` and
`--from-cluster`.

## Azure

`just azure-smoke` starts Azurite (`--profile azure`) and tests the Azure backend: upload, copy and
download. It needs a backup in the bucket first.

## What's validated

Every smoke runs against a local stack: Neo4j 2026.05 Enterprise in Docker, S3 emulated by floci.

| Area | Validated on | Not validated |
|---|---|---|
| Neo4j topology | one server; a three-server cluster in Docker; two servers for a cross-cluster restore | a production cluster across hosts or zones |
| Object store | S3 API on floci; Azure on Azurite; GCS against fakes | real S3, Azure Blob and GCS; SSE-KMS with a real key |
| Orchestrators | Dagster 1.13 in-process and on its Helm chart in k3d; Airflow 3.2 with `dag.test` | Airflow on its Helm chart with KubernetesExecutor; Dagster+ |
| Runner | subprocess in a container; `RUNNER_MODE=k8s` on k3d for both orchestrators | k8s on a managed cluster with IRSA or workload identity |
| CLI and MCP | a smoke for every command (`just cli-smoke`, `ops-smoke`, `mcp-smoke`, ...) | |
