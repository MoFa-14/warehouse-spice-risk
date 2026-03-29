"""Feature extraction for analogue similarity matching."""

from __future__ import annotations

from math import cos, pi, sin

from forecasting.models import FeatureVector, TimeSeriesPoint
from forecasting.utils import linear_regression_slope, population_std, to_utc_iso


def extract_feature_vector(window: list[TimeSeriesPoint]) -> FeatureVector:
    """Build the fixed feature vector for the current 3-hour baseline window."""
    if not window:
        raise ValueError("Cannot extract forecasting features from an empty window.")

    temps = [point.temp_c for point in window]
    rhs = [point.rh_pct for point in window]
    dews = [point.dew_point_c for point in window]
    observed_points = sum(1 for point in window if point.observed)
    missing_rate = 1.0 - (observed_points / float(len(window)))

    features = {
        "temp_last": temps[-1],
        "rh_last": rhs[-1],
        "dew_last": dews[-1],
        "temp_slope_15": _slope_tail(temps, 15),
        "temp_slope_30": _slope_tail(temps, 30),
        "temp_slope_60": _slope_tail(temps, 60),
        "rh_slope_15": _slope_tail(rhs, 15),
        "rh_slope_30": _slope_tail(rhs, 30),
        "rh_slope_60": _slope_tail(rhs, 60),
        "dew_slope_30": _slope_tail(dews, 30),
        "temp_std_30": population_std(temps[-30:]),
        "temp_std_60": population_std(temps[-60:]),
        "rh_std_30": population_std(rhs[-30:]),
        "rh_std_60": population_std(rhs[-60:]),
        "temp_min_60": min(temps[-60:]),
        "temp_max_60": max(temps[-60:]),
        "rh_min_60": min(rhs[-60:]),
        "rh_max_60": max(rhs[-60:]),
    }
    hour_fraction = (window[-1].ts_utc.hour * 60 + window[-1].ts_utc.minute) / 1440.0
    features["hour_sin"] = sin(2.0 * pi * hour_fraction)
    features["hour_cos"] = cos(2.0 * pi * hour_fraction)

    return FeatureVector(
        ts_pc_utc=to_utc_iso(window[-1].ts_utc),
        values=features,
        missing_rate=missing_rate,
        observed_points=observed_points,
    )


def _slope_tail(values: list[float], count: int) -> float:
    tail = values[-min(count, len(values)) :]
    return linear_regression_slope(tail)
