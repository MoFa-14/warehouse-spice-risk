# File overview:
# - Responsibility: Run the gateway scanner without installing the package first.
# - Project role: Provides standalone inspection utilities for transport and service
#   debugging.
# - Main data or concerns: Discovery outputs, scanned services, and diagnostic
#   prints.
# - Related flow: Runs operator-invoked diagnostics against gateway transport state.
# - Why this matters: Standalone tools matter because they expose transport state
#   without changing the main runtime path.

"""Run the gateway scanner without installing the package first."""

from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.main import cli


if __name__ == "__main__":
    raise SystemExit(cli(["--scan-only", *sys.argv[1:]]))
