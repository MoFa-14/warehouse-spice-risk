"""Dump GATT services for one matching pod without installing the package first."""

from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.main import cli


if __name__ == "__main__":
    raise SystemExit(cli(["--dump-services", *sys.argv[1:]]))
