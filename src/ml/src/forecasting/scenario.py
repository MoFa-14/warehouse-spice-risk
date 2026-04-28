# File overview:
# - Responsibility: Alternate event-persist scenario generation.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Alternate event-persist scenario generation.

Responsibilities:
- Builds the cautionary scenario used when recent telemetry looks disturbance
  driven.
- Sits beside the baseline analogue forecast rather than replacing it.
- Uses raw recent behaviour directly so the alternate path reflects the current
  disturbance instead of the filtered baseline state.

Project flow:
- recent-event detection -> event-persist scenario generation -> stored bundle
  -> dashboard comparison against the baseline scenario
"""

from __future__ import annotations

from math import sqrt

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import clamp, population_std


# Event-persist forecast builder
# - Purpose: constructs the alternate scenario that extrapolates the current
#   disturbance for a short horizon.
# - Project role: scenario-generation stage triggered only after recent-event
#   detection.
# - Inputs: raw recent history and event-specific rate limits.
# - Outputs: ``ForecastTrajectory`` for the ``event_persist`` scenario.
# - Important decision: uses raw recent slopes rather than the filtered
#   baseline window because this scenario intentionally explores continued
#   disturbance conditions.
# Function purpose: Continue the current short-term raw slope as a
#   disturbance-persist scenario.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as raw_window, config, interpreted according to the
#   implementation below.
# - Outputs: Returns ForecastTrajectory when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def build_event_persist_forecast(
    raw_window: list[TimeSeriesPoint],
    *,
    config: ForecastConfig,
) -> ForecastTrajectory:
    """Continue the current short-term raw slope as a disturbance-persist scenario."""
    temp_last = raw_window[-1].temp_c
    rh_last = raw_window[-1].rh_pct
    lookback = min(5, len(raw_window) - 1)
    temp_rate = 0.0
    rh_rate = 0.0
    if lookback > 0:
        # The recent 5-minute slope acts as the disturbance continuation signal,
        # but rate caps keep the alternate path bounded and operationally
        # interpretable.
        temp_rate = clamp(
            (raw_window[-1].temp_c - raw_window[-1 - lookback].temp_c) / float(lookback),
            -config.event_temp_rate_cap_c_per_min,
            config.event_temp_rate_cap_c_per_min,
        )
        rh_rate = clamp(
            (raw_window[-1].rh_pct - raw_window[-1 - lookback].rh_pct) / float(lookback),
            -config.event_rh_rate_cap_pct_per_min,
            config.event_rh_rate_cap_pct_per_min,
        )

    temp_std = max(population_std([point.temp_c for point in raw_window[-30:]]), config.fallback_temp_band_c)
    rh_std = max(population_std([point.rh_pct for point in raw_window[-30:]]), config.fallback_rh_band_pct)
    temp_forecast: list[float] = []
    rh_forecast: list[float] = []
    dew_forecast: list[float] = []
    temp_p25: list[float] = []
    temp_p75: list[float] = []
    rh_p25: list[float] = []
    rh_p75: list[float] = []
    # RH drift decays over the horizon so the scenario remains cautionary
    # without extending a sharp transient as a fully linear 30-minute ramp.
    rh_drift = 0.0

    for step in range(1, config.horizon_minutes + 1):
        widening = 1.0 + 0.5 * sqrt(step / float(config.horizon_minutes))
        temp_value = temp_last + temp_rate * step
        rh_drift = clamp(
            rh_drift + rh_rate * (config.event_rh_decay_per_step ** (step - 1)),
            -config.event_rh_max_total_drift_pct,
            config.event_rh_max_total_drift_pct,
        )
        rh_value = clamp(rh_last + rh_drift, 0.0, 100.0)
        temp_forecast.append(temp_value)
        rh_forecast.append(rh_value)
        # Dew point is always derived from the scenario's temperature and RH so
        # the alternate path stays physically consistent.
        dew_forecast.append(calculate_dew_point_c(temp_value, rh_value))
        temp_p25.append(temp_value - temp_std * widening)
        temp_p75.append(temp_value + temp_std * widening)
        rh_p25.append(max(0.0, rh_value - rh_std * widening))
        rh_p75.append(min(100.0, rh_value + rh_std * widening))

    return ForecastTrajectory(
        scenario="event_persist",
        temp_forecast_c=temp_forecast,
        rh_forecast_pct=rh_forecast,
        dew_point_forecast_c=dew_forecast,
        temp_p25_c=temp_p25,
        temp_p75_c=temp_p75,
        rh_p25_pct=rh_p25,
        rh_p75_pct=rh_p75,
        source="event_persist_slope",
        neighbor_count=0,
        case_count=0,
        notes=(
            "Continues the current 5-minute raw slope with deterministic rate caps, "
            "exponential RH decay, and a capped total RH drift."
        ),
    )
