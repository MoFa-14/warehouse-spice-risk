"""Dew point helpers shared by the forecasting pipeline."""

from __future__ import annotations

from math import log


def calculate_dew_point_c(temp_c: float, rh_pct: float) -> float:
    """Compute dew point in Celsius using the Magnus approximation."""
    rh_fraction = max(0.01, min(100.0, float(rh_pct))) / 100.0
    a = 17.625
    b = 243.04
    gamma = (a * float(temp_c) / (b + float(temp_c))) + log(rh_fraction)
    return (b * gamma) / (a - gamma)
