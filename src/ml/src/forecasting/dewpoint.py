# File overview:
# - Responsibility: Dew point helpers shared by the forecasting pipeline.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Dew point helpers shared by the forecasting pipeline.

The project does not train a separate dew-point forecasting model. Instead,
dew point is treated as a physically meaningful *derived* quantity that is
recomputed from temperature and relative humidity whenever needed.

That design matters in a viva because it means:
- if the dew-point forecast is poor, the root cause is usually upstream in the
  temperature or RH forecast
- the code is enforcing a thermodynamic relationship between variables instead
  of letting three separate forecasts drift apart inconsistently
"""

from __future__ import annotations

from math import log
# Function purpose: Compute dew point in Celsius using the Magnus approximation.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as temp_c, rh_pct, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def calculate_dew_point_c(temp_c: float, rh_pct: float) -> float:
    """Compute dew point in Celsius using the Magnus approximation.

    This function appears in several stages of the project:
    - when raw telemetry is cleaned and resampled
    - when baseline and event-persist forecast trajectories are built
    - when persistence baselines are created for evaluation and dashboard plots

    In other words, dew point is always kept consistent with the currently
    assumed temperature and RH values. The small RH clamp avoids taking the
    logarithm of zero and also prevents numerical instability for pathological
    inputs.
    """
    rh_fraction = max(0.01, min(100.0, float(rh_pct))) / 100.0
    a = 17.625
    b = 243.04
    gamma = (a * float(temp_c) / (b + float(temp_c))) + log(rh_fraction)
    return (b * gamma) / (a - gamma)
