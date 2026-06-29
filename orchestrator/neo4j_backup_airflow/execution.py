"""Execution dispatcher for neo4j-admin commands: subprocess (VM/EC2, the validated
default) or a KubernetesPodOperator pod, per RUNNER_MODE. The Airflow analogue of the
Dagster `_run_admin` Pipes split — backup and AVP DAGs route every neo4j-admin call
through `run_admin`, so the subprocess/k8s choice lives in one place.

k8s mode is exercised against k3d in airflow/smoke_k8s.py (#10). The pod shape (ephemeral
scratch PVC, memory cap, node selector) comes straight off the core BackupRunner, the same
fields the Dagster adapter feeds to PipesK8sClient — see neo4j_backup_core/clients.py.
"""

import json
import os
import subprocess

from neo4j_backup_airflow import config


def run_admin(command: list, env: dict | None = None) -> None:
    """Run one neo4j-admin command to completion; non-zero exit raises."""
    runner = config.runner()
    full_env = {**runner.env(), **(env or {})}
    if runner.mode == "k8s":
        _run_pod(runner, command, full_env)
    else:
        subprocess.run(command, check=True, env={**os.environ, **full_env})


def _run_pod(runner, command: list, env: dict) -> None:
    if not runner.image:
        raise RuntimeError("RUNNER_MODE=k8s requires RUNNER_IMAGE")
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.sdk import get_current_context
    from kubernetes.client import models as k8s

    env = {**json.loads(runner.extra_env_json or "{}"), **env}
    node_selector = json.loads(runner.node_selector_json or "{}")
    # k8s client >=27 split PVC requests into V1VolumeResourceRequirements; fall back for older.
    vol_reqs = getattr(k8s, "V1VolumeResourceRequirements", k8s.V1ResourceRequirements)
    scratch = k8s.V1Volume(
        name="scratch",
        ephemeral=k8s.V1EphemeralVolumeSource(
            volume_claim_template=k8s.V1PersistentVolumeClaimTemplate(
                spec=k8s.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=vol_reqs(requests={"storage": runner.scratch_storage}),
                ))),
    )
    op = KubernetesPodOperator(
        task_id="neo4j_admin_pod",
        name="neo4j-admin",
        namespace=os.environ.get("RUNNER_NAMESPACE", "default"),
        image=runner.image,
        image_pull_policy="IfNotPresent",
        cmds=command[:1],
        arguments=command[1:],
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in env.items()],
        container_resources=k8s.V1ResourceRequirements(limits={"memory": runner.memory_limit}),
        volumes=[scratch],
        volume_mounts=[k8s.V1VolumeMount(name="scratch", mount_path=runner.scratch_path)],
        node_selector=node_selector or None,
        service_account_name=runner.service_account or None,
        in_cluster=os.environ.get("RUNNER_IN_CLUSTER", "true").lower() == "true",
        on_finish_action="delete_pod",
        get_logs=True,
    )
    # KPO derives its pod labels from the live task context (dag_id/run_id/try_number);
    # run_admin is only ever called from inside a task, so the context is available.
    op.execute(get_current_context())  # blocks until the pod terminates, raises on failure
