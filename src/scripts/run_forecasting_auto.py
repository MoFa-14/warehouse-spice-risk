# File overview:
# - Responsibility: Start the unified automatic forecasting loop.
# - Project role: Provides convenience entry points for monitoring, forecasting, and
#   evaluation workflows.
# - Main data or concerns: Command-line options, runtime handles, and script-level
#   control flow.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.
# - Why this matters: Scripts matter because they are the shortest operational path
#   into the project for routine runs.

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
# Function purpose: Dispatches the top-level script entry point and forwards
#   command-line arguments into the underlying runtime path.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as argv, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv or sys.argv[1:])
    if not arguments:
        arguments = ["run", "--all", "--every-minutes", "5"]
    return cli(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
