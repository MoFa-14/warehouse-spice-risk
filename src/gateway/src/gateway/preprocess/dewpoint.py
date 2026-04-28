# File overview:
# - Responsibility: Derived dew-point feature computation for processed datasets.
# - Project role: Cleans, resamples, derives, or exports telemetry into
#   analysis-ready forms.
# - Main data or concerns: Time-series points, derived psychrometric variables, and
#   resampled grids.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.
# - Why this matters: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.

"""Derived dew-point feature computation for processed datasets."""

from __future__ import annotations

import math
# Function purpose: Compute dew point in Celsius using the requested Magnus
#   approximation.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as temp_c, rh_pct, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def dew_point_c(temp_c: float | None, rh_pct: float | None) -> float | None:
    """Compute dew point in Celsius using the requested Magnus approximation."""
    if temp_c is None or rh_pct is None:
        return None

    rh = max(1e-6, min(rh_pct, 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * temp_c / (b + temp_c)) + math.log(rh)
    return (b * gamma) / (a - gamma)
