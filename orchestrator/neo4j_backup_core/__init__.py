"""Orchestrator-agnostic core: naming, policy, storage paths, and plain clients.

No Dagster or Airflow imports — both adapters wrap this. `naming` is importable on its
own (its parity test runs without either orchestrator installed).
"""

# Single source of truth for the package version (pyproject derives it via
# [tool.setuptools.dynamic]). Bump this on release; see RELEASING.md.
__version__ = "0.3.0"

from . import naming  # noqa: F401,E402
from . import metadata  # noqa: F401,E402
