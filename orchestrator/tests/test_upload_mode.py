"""#218: BACKUP_UPLOAD=pipeline with RUNNER_MODE=k8s is refused; no artifact is never success."""

import types

import pytest

from neo4j_backup_core import ops
from neo4j_backup_core import policy as P

TIERS = {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}


def test_env_combination_refused():
    with pytest.raises(ValueError, match="RUNNER_MODE=k8s"):
        P.check_upload_mode(environ={"RUNNER_MODE": "k8s", "BACKUP_UPLOAD": "pipeline"})
    P.check_upload_mode(environ={"RUNNER_MODE": "subprocess", "BACKUP_UPLOAD": "pipeline"})
    P.check_upload_mode(environ={"RUNNER_MODE": "k8s", "BACKUP_UPLOAD": "admin"})


def test_cluster_setting_refused_on_k8s():
    pol = P.Policy.model_validate({
        "clusters": {"west": {"bolt_uri": "neo4j://w:7687", "backup_upload": "pipeline"}},
        "db_groups": [{"id": "g", "tier": "gold", "aliases": ["g1"], "cluster": "west"}],
        "tiers": TIERS})
    with pytest.raises(ValueError, match="cluster 'west'"):
        P.check_upload_mode(pol, environ={"RUNNER_MODE": "k8s"})


def test_runtime_guard_and_no_artifact():
    with pytest.raises(ops.OpError, match="RUNNER_MODE=k8s"):
        ops._upload(types.SimpleNamespace(mode="k8s", backup_upload=None), "pipeline")

    class Store:
        def uri(self, p):
            return f"s3://b/{p}"

        def latest_artifact_key(self, p):
            return None

    runner = types.SimpleNamespace(backup_command=lambda db, to, kind: ["backup", db])
    with pytest.raises(ops.OpError, match="no artifact"):
        ops.run_backup(lambda cmd, env=None: "", runner, Store(), "orders", "g/o/p/", "FULL")
