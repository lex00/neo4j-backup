"""Orchestrator-agnostic core: naming, policy, storage paths, and plain clients.

No Dagster or Airflow imports — both adapters wrap this. `naming` is importable on its
own (its parity test runs without either orchestrator installed).
"""

from . import naming  # noqa: F401
