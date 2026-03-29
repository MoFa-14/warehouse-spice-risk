"""Scenario generation for baseline and event-persist forecasts."""

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
    """Continue the current short-term raw slope as a disturbance-persist scenario."""
    temp_last = raw_window[-1].temp_c
    rh_last = raw_window[-1].rh_pct
    lookback = min(5, len(raw_window) - 1)
    temp_rate = 0.0
    rh_rate = 0.0
    if lookback > 0:
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

    for step in range(1, config.horizon_minutes + 1):
        widening = 1.0 + 0.5 * sqrt(step / float(config.horizon_minutes))
        temp_value = temp_last + temp_rate * step
        rh_value = clamp(rh_last + rh_rate * step, 0.0, 100.0)
        temp_forecast.append(temp_value)
        rh_forecast.append(rh_value)
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
        notes="Continues the current 5-minute raw slope with deterministic rate caps.",
    )
