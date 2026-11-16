"""#229: k8s pods get credentials from Secrets, and namespace/requests/tolerations."""

import pytest

from neo4j_backup_core.clients import BackupRunner


def test_credentials_never_go_into_the_pod_spec_as_text():
    r = BackupRunner(mode="k8s", image="neo4j:x", extra_env_json='{"AWS_REGION": "eu-west-1"}')
    with pytest.raises(RuntimeError, match="AWS_SECRET_ACCESS_KEY would go into the pod spec"):
        r.k8s_pod_spec({"AWS_SECRET_ACCESS_KEY": "s3cr3t", "OUTPUT_URI": "s3://b/x/"})
    r = BackupRunner(mode="k8s", image="neo4j:x",
                     secret_env_json='{"AWS_SECRET_ACCESS_KEY": "backup-creds:secret-key"}')
    spec = r.k8s_pod_spec({"AWS_SECRET_ACCESS_KEY": "s3cr3t", "OUTPUT_URI": "s3://b/x/"})
    env = spec["containers"][0]["env"]
    assert {"name": "OUTPUT_URI", "value": "s3://b/x/"} in env
    assert {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {
        "name": "backup-creds", "key": "secret-key"}}} in env
    assert "s3cr3t" not in str(spec)


def test_requests_and_tolerations():
    r = BackupRunner(mode="k8s", cpu_request="2", memory_request="3Gi",
                     tolerations_json='[{"key": "backup", "operator": "Exists"}]')
    spec = r.k8s_pod_spec({})
    assert spec["containers"][0]["resources"] == {"limits": {"memory": "4Gi"},
                                                  "requests": {"cpu": "2", "memory": "3Gi"}}
    assert spec["tolerations"] == [{"key": "backup", "operator": "Exists"}]
