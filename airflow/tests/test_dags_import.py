"""CI-friendly check: every DAG in airflow/dags parses with no import errors.

Needs no live Neo4j/S3 — DAG modules must not connect at parse time (only inside tasks).
"""

import os
from pathlib import Path

from airflow.models import DagBag


def test_no_import_errors():
    repo = Path(__file__).resolve().parents[2]
    os.environ.setdefault("NEO4J_BACKUP_POLICY", str(repo / "policies" / "demo.yaml"))
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert bag.dags, "no DAGs discovered"


def test_bad_policy_is_an_import_error(tmp_path, monkeypatch):
    # #75: a policy that fails to load shows as an import error, not as missing DAGs
    bad = tmp_path / "bad.yaml"
    bad.write_text("db_groups: [ {id: x, aliases: [a], tier: nope, s3_prefix: 's3://x/'} ]\n"
                   "tiers: {}\n")
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(bad))
    from neo4j_backup_core import policy
    policy._cache.clear()
    repo = Path(__file__).resolve().parents[2]
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    errors = {Path(k).name: v for k, v in bag.import_errors.items()}
    assert "backup_dag.py" in errors and "unknown tier 'nope'" in errors["backup_dag.py"]
