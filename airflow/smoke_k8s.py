"""Validate RUNNER_MODE=k8s for the Airflow adapter: the backup task's execution
dispatcher launches a KubernetesPodOperator pod in k3d, that pod runs neo4j-admin against
the Compose Neo4j (host IP) and writes an artifact to the Compose MinIO, on a fresh
ephemeral scratch PVC. The Airflow analogue of orchestrator/smoke_k8s.py.

Prereqs: `just up` (Compose, 6362 published) + `just k3d-up` (cluster + image import) +
`just bootstrap` (demo group). Drives the real `neo4j_backup_gold_full` DAG via dag.test()
so every neo4j-admin step runs as a KubernetesPodOperator pod inside a live task context
(KPO derives its pod labels from dag_id/run_id/try_number). The gold tier fans out to its
aliases — each one a separate pod in k3d.

    airflow/.venv/bin/python airflow/smoke_k8s.py
"""

import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)


def _host_ip() -> str:
    """The host IP a k3d pod can reach (the published Compose ports). `host.k3d.internal`
    is in CoreDNS but neo4j-admin's resolver didn't use it, so target the IP directly."""
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
IMAGE = f"neo4j:{os.environ.get('NEO4J_VERSION', '2026.05.0-enterprise')}"

os.environ.update({
    "AIRFLOW_HOME": os.path.join(REPO, ".airflow_home"),
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "NEO4J_BACKUP_POLICY": os.path.join(REPO, "policies", "demo.yaml"),
    "NEO4J_BOLT_URI": "neo4j://localhost:7687",
    "NEO4J_PASSWORD": "devpassword",
    "BACKUP_BUCKET": "neo4j-backups",
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000",  # host-side (snapshot/list)
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    # --- k8s execution mode ---
    "RUNNER_MODE": "k8s",
    "RUNNER_IMAGE": IMAGE,
    "RUNNER_IN_CLUSTER": "false",                    # dag runs on host -> use kubeconfig
    "NEO4J_BACKUP_SOURCE": f"{HOST_IP}:6362",        # pod -> host Neo4j backup port
    "RUNNER_PAGECACHE": "256M",
    "RUNNER_HEAP_SIZE": "512M",
    "RUNNER_MEMORY_LIMIT": "1Gi",
    "RUNNER_SCRATCH_STORAGE": "1Gi",
    "RUNNER_EXTRA_ENV": json.dumps({                 # pod -> host MinIO
        "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL_S3": f"http://{HOST_IP}:9000",
    }),
})


AF = os.path.join(REPO, "airflow", ".venv", "bin", "airflow")


def main() -> None:
    subprocess.run([AF, "db", "migrate"], check=True, env=os.environ,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([AF, "pools", "set", "neo4j_full", "1", "full lane"], check=True,
                   env=os.environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import paths
    import backup_dag as bd

    store, neo = config.store(), config.neo4j()
    phys = neo.alias_target("acme-orders")
    assert phys, "alias acme-orders has no target — run `just bootstrap` first"
    prefix = paths.physical_prefix("demo", "acme-orders", phys)
    before = len(store.list_artifacts(prefix))

    print(f"== RUNNER_MODE=k8s gold-tier backup (pods in k3d, host {HOST_IP}) ==")
    run = bd.neo4j_backup_gold_full.test()  # each task -> run_admin -> KubernetesPodOperator
    state = str(getattr(run, "state", run)).split(".")[-1].lower()
    assert state == "success", f"k8s-mode backup DAG state={state}"

    after = len(store.list_artifacts(prefix))
    print(f"== artifacts under {prefix}: {before} -> {after} ==")
    assert after > before, "the k8s backup pods did not write an artifact"
    print("PASS: RUNNER_MODE=k8s — Airflow dispatched pods in k3d that ran neo4j-admin")


if __name__ == "__main__":
    main()
