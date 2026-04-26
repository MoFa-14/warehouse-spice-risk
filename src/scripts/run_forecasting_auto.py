"""Start the unified automatic forecasting loop.

When no extra CLI arguments are provided, this script runs the recurring
forecaster for every known pod and lets the live runner handle:
- due forecast evaluation
- case-base backfilling / retraining
- recent-bias calibration
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[2]
for package_root in (ROOT / "src" / "gateway" / "src", ROOT / "src" / "ml" / "src"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from gateway.cli.forecast_cli import cli


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv or sys.argv[1:])
    if not arguments:
        arguments = ["run", "--all", "--every-minutes", "5"]
    return cli(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
