"""Scenario generation for the alternate event-persist forecast.

The baseline forecast answers:
"What usually happens next under normal continuation?"

The event-persist scenario answers a different operational question:
"What if the current disturbance continues for a while?"

This file therefore produces the more cautionary second scenario that is shown
when recent telemetry looks event-like.
"""

from __future__ import annotations

from math import sqrt

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import clamp, population_std


def build_event_persist_forecast(
    raw_window: list[TimeSeriesPoint],
    *,
    config: ForecastConfig,
) -> ForecastTrajectory:
    """Continue the current short-term raw slope as a disturbance-persist scenario.

    This function is triggered only when recent-event detection says the latest
    readings are unusual. It uses the *raw* recent history, not the filtered
    baseline window, because the whole purpose is to explore what happens if the
    disturbance itself persists.
    """
    temp_last = raw_window[-1].temp_c
    rh_last = raw_window[-1].rh_pct
    lookback = min(5, len(raw_window) - 1)
    temp_rate = 0.0
    rh_rate = 0.0
    if lookback > 0:
        # The event scenario is intentionally simple and explainable: it starts
        # from the recent 5-minute slope, then constrains that slope with rate
        # caps so the what-if path stays interpretable.
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
    # RH now uses a decaying drift rather than a full straight-line continuation
    # for the whole 30 minutes. This keeps the scenario cautionary but less
    # unrealistically explosive.
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
        # Dew point is derived from the scenario's temperature and RH so the
        # alternate scenario stays physically self-consistent.
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
