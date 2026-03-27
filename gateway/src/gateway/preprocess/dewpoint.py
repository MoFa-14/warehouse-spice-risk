"""Derived dew-point feature computation for processed datasets."""

from __future__ import annotations

import math


def dew_point_c(temp_c: float | None, rh_pct: float | None) -> float | None:
    """Compute dew point in Celsius using the requested Magnus approximation."""
    if temp_c is None or rh_pct is None:
        return None

    rh = max(1e-6, min(rh_pct, 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * temp_c / (b + temp_c)) + math.log(rh)
    return (b * gamma) / (a - gamma)
