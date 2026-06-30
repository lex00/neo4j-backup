"""Validate RUNNER_MODE=k8s end to end: the backup asset launches a pod in k3d via
PipesK8sClient, that pod runs neo4j-admin against the Compose Neo4j (host.k3d.internal)
and writes an SSE-KMS artifact to the Compose MinIO, with a fresh ephemeral scratch PVC.

Prereqs: `just up` (Compose, 6362 published) + `just k3d-up` (cluster + image import) +
the demo group bootstrapped (`just bootstrap`).

    orchestrator/.venv/bin/python orchestrator/smoke_k8s.py
"""

import json
import os
import subprocess

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

import dagster as dg

from neo4j_backup_dagster import definitions as D
from neo4j_backup_dagster.resources import Neo4jResource, ObjectStoreResource, RunnerResource

IMAGE = f"neo4j:{os.environ.get('NEO4J_VERSION', '2026.05.0-enterprise')}"


def _host_ip() -> str:
    """The host IP a k3d pod can reach (the published Compose ports). `host.k3d.internal`
    is in CoreDNS but neo4j-admin's resolver didn't use it, so we target the IP directly."""
    if os.environ.get("K3D_HOST_IP"):
        return os.environ["K3D_HOST_IP"]
    out = subprocess.run(
        ["kubectl", "get", "cm", "coredns", "-n", "kube-system",
         "-o", "jsonpath={.data.NodeHosts}"],
        capture_output=True, text=True,
    ).stdout
    for line in out.splitlines():
        if "host.k3d.internal" in line:
            return line.split()[0]
    return "host.k3d.internal"


HOST_IP = _host_ip()
POD_S3 = f"http://{HOST_IP}:9000"
POD_SOURCE = f"{HOST_IP}:6362"
PARTITION = "demo/acme-orders"

RES = {
    "neo4j": Neo4jResource(uri="neo4j://localhost:7687", user="neo4j", password="devpassword"),
    "store": ObjectStoreResource(bucket="neo4j-backups", endpoint_url="http://localhost:9000", region="us-east-1"),
    "runner": RunnerResource(
        mode="k8s", image=IMAGE, backup_source=POD_SOURCE,
        scratch_path="/scratch", pagecache="256M", heap_size="512M",
        memory_limit="1Gi", scratch_storage="1Gi",
        extra_env_json=json.dumps({
            "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL_S3": POD_S3,
        }),
    ),
    "pipes_subprocess_client": dg.PipesSubprocessClient(),
}


def main() -> None:
    neo4j: Neo4jResource = RES["neo4j"]
    store: ObjectStoreResource = RES["store"]
    phys = neo4j.alias_target("acme-orders")
    assert phys, "alias acme-orders has no target — run `just bootstrap` first"
    prefix = D._physical_prefix("demo", "acme-orders", phys)
    before = len(store.list_artifacts(prefix))

    print(f"== RUNNER_MODE=k8s backup of {phys} (pod in k3d via PipesK8sClient) ==")
    with dg.instance_for_test() as inst:
        inst.add_dynamic_partitions(D.targets.name, [PARTITION])
        r = dg.materialize(
            [D.backup], partition_key=PARTITION, resources=RES, instance=inst,
            run_config={"ops": {"backup": {"config": {"kind": "FULL"}}}},
        )
        assert r.success, "k8s-mode backup failed"

    after = len(store.list_artifacts(prefix))
    print(f"== artifacts under {prefix}: {before} -> {after} ==")
    assert after > before, "the k8s backup pod did not write an artifact"
    print("PASS: RUNNER_MODE=k8s — pod ran neo4j-admin in k3d and wrote to the store")


if __name__ == "__main__":
    main()
