# File overview:
# - Responsibility: Feature extraction for analogue similarity matching.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Feature extraction for analogue similarity matching.

Responsibilities:
- Converts a 3-hour minute-level telemetry window into a compact feature vector.
- Sits between baseline-window preparation and historical case matching.
- Encodes current state, short-term trend, local variability, operating range,
  and time-of-day context in a form the analogue model can compare directly.

Project flow:
- telemetry history -> baseline-safe window -> feature vector -> neighbour
  search -> forecast trajectory

Design reason:
- The analogue model compares interpretable summary features instead of raw
  point-by-point sequences so that similarity decisions remain inspectable and
  stable across windows.
"""

from __future__ import annotations

from math import cos, pi, sin

from forecasting.models import FeatureVector, TimeSeriesPoint
from forecasting.utils import linear_regression_slope, population_std, to_utc_iso


# Feature-vector construction
# - Purpose: compresses one baseline-safe 3-hour window into the fixed features
#   used by analogue similarity matching.
# - Project role: transformation stage between window preprocessing and
#   neighbour scoring.
# - Inputs: minute-level ``TimeSeriesPoint`` objects after event-robust
#   filtering.
# - Outputs: ``FeatureVector`` metadata plus named scalar features.
# - Important decisions: keeps features interpretable, records missing-rate
#   evidence, and captures both short and medium recent trends.
# - Downstream dependency: the kNN forecaster uses these feature values as the
#   anchor for distance calculations and persistence fallbacks.
# Function purpose: Build the fixed feature vector for the current 3-hour baseline
#   window.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as window, interpreted according to the implementation
#   below.
# - Outputs: Returns FeatureVector when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def extract_feature_vector(window: list[TimeSeriesPoint]) -> FeatureVector:
    """Build the fixed feature vector for the current 3-hour baseline window."""
    if not window:
        raise ValueError("Cannot extract forecasting features from an empty window.")

    temps = [point.temp_c for point in window]
    rhs = [point.rh_pct for point in window]
    dews = [point.dew_point_c for point in window]
    observed_points = sum(1 for point in window if point.observed)
    missing_rate = 1.0 - (observed_points / float(len(window)))

    # Anchor-state features describe the current storage regime at the forecast
    # timestamp. They later act as the most direct "where are conditions now?"
    # reference during analogue matching.
    features = {
        "temp_last": temps[-1],
        "rh_last": rhs[-1],
        "dew_last": dews[-1],
        # Multi-scale slope features separate a brief tail movement from a more
        # sustained drift across the recent history window.
        "temp_slope_15": _slope_tail(temps, 15),
        "temp_slope_30": _slope_tail(temps, 30),
        "temp_slope_60": _slope_tail(temps, 60),
        "rh_slope_15": _slope_tail(rhs, 15),
        "rh_slope_30": _slope_tail(rhs, 30),
        "rh_slope_60": _slope_tail(rhs, 60),
        "dew_slope_30": _slope_tail(dews, 30),
        # Variability and range features distinguish steady storage periods from
        # unstable ones even when the latest value appears similar.
        "temp_std_30": population_std(temps[-30:]),
        "temp_std_60": population_std(temps[-60:]),
        "rh_std_30": population_std(rhs[-30:]),
        "rh_std_60": population_std(rhs[-60:]),
        "temp_min_60": min(temps[-60:]),
        "temp_max_60": max(temps[-60:]),
        "rh_min_60": min(rhs[-60:]),
        "rh_max_60": max(rhs[-60:]),
    }
    # Cyclical hour encoding adds a lightweight daily-context signal without
    # requiring a separate seasonal model or a discontinuous hour index.
    hour_fraction = (window[-1].ts_utc.hour * 60 + window[-1].ts_utc.minute) / 1440.0
    features["hour_sin"] = sin(2.0 * pi * hour_fraction)
    features["hour_cos"] = cos(2.0 * pi * hour_fraction)

    return FeatureVector(
        ts_pc_utc=to_utc_iso(window[-1].ts_utc),
        values=features,
        missing_rate=missing_rate,
        observed_points=observed_points,
    )


# Tail-slope helper
# - Purpose: estimates the recent slope over a configurable trailing slice.
# - Design reason: least-squares slope is less sensitive to one noisy endpoint
#   than a simple start-to-end difference.
# Function purpose: Return the least-squares slope over the most recent tail of a
#   series.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, count, interpreted according to the
#   implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _slope_tail(values: list[float], count: int) -> float:
    """Return the least-squares slope over the most recent tail of a series."""
    tail = values[-min(count, len(values)) :]
    return linear_regression_slope(tail)
