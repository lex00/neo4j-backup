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
