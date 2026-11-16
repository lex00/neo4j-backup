"""Parity test: naming.py must match bootstrap/naming.sh byte-for-byte.

Runnable two ways:
  python3 orchestrator/tests/test_naming_parity.py     # standalone, prints PASS/FAIL
  pytest orchestrator/tests/test_naming_parity.py      # as a test
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
NAMING_SH = REPO / "bootstrap" / "naming.sh"
sys.path.insert(0, str(REPO / "orchestrator"))

from neo4j_backup_core import naming  # noqa: E402

# Inputs spanning clean, uppercase, underscores, dots, symbols, whitespace, overflow.
CASES = [
    "acme-orders",
    "acme-graph",
    "Acme_Orders.EU",
    "weird name!! ",
    "  leading-dash",
    "UPPER",
    "a.b.c",
    "x" * 70,
    "tenant_42",
    "Customer Graph (EU) – production instance number 42",  # #87: hash kept when cut
    "y" * 200,
    "2024-orders",   # #85: digit-led slug, db- prefix in the physical only
    "Orders-",       # trailing dash
    "9lives",
]
TS = "20260628t120000"


def _sh(func: str, arg: str) -> str:
    """Run one naming.sh function and return its stdout (notes go to stderr)."""
    out = subprocess.run(
        ["bash", "-c", f'source "{NAMING_SH}"; {func} "$1"', "_", arg],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def _sh_physical(arg: str) -> str:
    out = subprocess.run(
        ["bash", "-c", f'source "{NAMING_SH}"; naming_physical "$1" "$2"', "_", arg, TS],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def check() -> list[str]:
    failures: list[str] = []
    for a in CASES:
        pairs = {
            "sanitize": (naming.sanitize(a), _sh("naming_sanitize", a)),
            "slug": (naming.slug(a), _sh("naming_slug", a)),
            "physical": (naming.physical(a, TS), _sh_physical(a)),
        }
        for name, (py, sh) in pairs.items():
            if py != sh:
                failures.append(f"{name}({a!r}): py={py!r} sh={sh!r}")
    return failures


def test_naming_parity():
    failures = check()
    assert not failures, "naming.py/naming.sh mismatch:\n" + "\n".join(failures)


if __name__ == "__main__":
    fails = check()
    if fails:
        print("FAIL:\n" + "\n".join(fails))
        sys.exit(1)
    print(f"PASS: naming.py matches naming.sh across {len(CASES)} inputs")


def test_ts_format_parity(monkeypatch):
    """#86: NAMING_TS_FMT gives the same timestamp in Python and the shell."""
    import os
    import subprocess as sp
    from datetime import datetime, timezone
    fmt = "%Y%m%d%H%M"
    monkeypatch.setenv("NAMING_TS_FMT", fmt)
    py = naming.ts(datetime.now(timezone.utc))
    sh = sp.run(["bash", "-c", f'source "{NAMING_SH}"; naming_ts'], capture_output=True,
                text=True, env={**os.environ, "NAMING_TS_FMT": fmt}).stdout.strip()
    assert len(py) == len(sh) == 12 and py[:10] == sh[:10]


def test_ts_format_with_the_join_character_is_refused(monkeypatch):
    import os
    import subprocess as sp
    import pytest
    monkeypatch.setenv("NAMING_TS_FMT", "%Y-%m-%d")
    with pytest.raises(naming.NamingError):
        naming.ts()
    out = sp.run(["bash", "-c", f'source "{NAMING_SH}"'], capture_output=True, text=True,
                 env={**os.environ, "NAMING_TS_FMT": "%Y-%m-%d"})
    assert out.returncode != 0 and "join character" in out.stderr
