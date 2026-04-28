# File overview:
# - Responsibility: Gateway forecasting integration helpers.
# - Project role: Connects stored telemetry to forecasting, persistence, evaluation,
#   and calibration behavior.
# - Main data or concerns: History windows, forecast bundles, evaluation rows, and
#   calibration metadata.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.
# - Why this matters: This layer defines the live forecast lifecycle that the rest
#   of the project interprets and stores.

"""Gateway forecasting integration helpers."""

from __future__ import annotations

import sys
from pathlib import Path
# Function purpose: Ensures that forecasting package exists before later logic
#   depends on it.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _ensure_forecasting_package() -> None:
    current = Path(__file__).resolve()
    for base in (current.parent, *current.parents):
        candidate = base / "ml" / "src"
        if (candidate / "forecasting").exists():
            candidate_text = str(candidate)
            if candidate_text not in sys.path:
                sys.path.insert(0, candidate_text)
            return


_ensure_forecasting_package()
