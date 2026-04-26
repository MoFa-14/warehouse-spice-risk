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
