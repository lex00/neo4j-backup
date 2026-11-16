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


def test_profiled_policy_dags_see_resolved_groups(tmp_path, monkeypatch):
    # #101: a group whose tier comes from a profile gets the tier's DAGs and is in its targets
    import yaml
    repo = Path(__file__).resolve().parents[2]
    raw = yaml.safe_load((repo / "policies" / "demo.yaml").read_text())
    raw["db_groups"].append({"id": "shop", "profile": "app-live", "aliases": ["shop"],
                             "s3_prefix": "s3://x/"})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    from neo4j_backup_core import policy
    policy._cache.clear()
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert "neo4j_backup_gold_full" in bag.dags
    from neo4j_backup_airflow import config
    gold = policy.load_policy(config.policy_path()).groups_for_tier("gold")  # the targets task
    assert "shop" in [g.id for g in gold]
