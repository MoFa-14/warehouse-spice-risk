"""Feature extraction for analogue similarity matching.

This file is where the prototype moves from "a sequence of recent readings" to
"a compact description of current warehouse behaviour".

That transition is central to the project. The analogue model does not compare
entire raw 3-hour sequences point by point. Instead, it compares feature
vectors that summarise:
- the latest state
- recent trends
- short-term variability
- recent min/max range
- time-of-day context
"""

from __future__ import annotations

from math import cos, pi, sin

from forecasting.models import FeatureVector, TimeSeriesPoint
from forecasting.utils import linear_regression_slope, population_std, to_utc_iso


def extract_feature_vector(window: list[TimeSeriesPoint]) -> FeatureVector:
    """Build the fixed feature vector for the current 3-hour baseline window.

    Inputs:
    - the cleaned 3-hour baseline window after any event-robust filtering

    Output:
    - a ``FeatureVector`` that can be compared against historical cases

    In viva terms, this is the point where raw telemetry becomes
    forecast-ready. The chosen features are intentionally interpretable rather
    than opaque latent variables, so the similarity logic can be defended in
    plain language.
    """
    if not window:
        raise ValueError("Cannot extract forecasting features from an empty window.")

    temps = [point.temp_c for point in window]
    rhs = [point.rh_pct for point in window]
    dews = [point.dew_point_c for point in window]
    observed_points = sum(1 for point in window if point.observed)
    missing_rate = 1.0 - (observed_points / float(len(window)))

    # The last values anchor the system in the current regime. These are the
    # most important features when we want to know "what warehouse state are we
    # in right now?"
    features = {
        "temp_last": temps[-1],
        "rh_last": rhs[-1],
        "dew_last": dews[-1],
        # Multiple slope windows let the model distinguish a short transient
        # from a more sustained trend.
        "temp_slope_15": _slope_tail(temps, 15),
        "temp_slope_30": _slope_tail(temps, 30),
        "temp_slope_60": _slope_tail(temps, 60),
        "rh_slope_15": _slope_tail(rhs, 15),
        "rh_slope_30": _slope_tail(rhs, 30),
        "rh_slope_60": _slope_tail(rhs, 60),
        "dew_slope_30": _slope_tail(dews, 30),
        # Variability and range features help separate steady storage conditions
        # from unstable periods, even when the latest value alone looks similar.
        "temp_std_30": population_std(temps[-30:]),
        "temp_std_60": population_std(temps[-60:]),
        "rh_std_30": population_std(rhs[-30:]),
        "rh_std_60": population_std(rhs[-60:]),
        "temp_min_60": min(temps[-60:]),
        "temp_max_60": max(temps[-60:]),
        "rh_min_60": min(rhs[-60:]),
        "rh_max_60": max(rhs[-60:]),
    }
    # Hour-of-day encoding gives the case-based model a lightweight notion of
    # daily rhythm without introducing a complicated seasonal model.
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
    """Return the least-squares slope over the most recent tail of a series.

    Using regression rather than a simple last-minus-first difference makes the
    slope estimate more robust to one noisy point at the end of the window.
    """
    tail = values[-min(count, len(values)) :]
    return linear_regression_slope(tail)
