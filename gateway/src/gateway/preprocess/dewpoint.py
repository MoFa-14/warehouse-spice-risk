"""Derived dew-point feature computation for processed datasets."""

from __future__ import annotations

import math


def dew_point_c(temp_c: float | None, rh_pct: float | None) -> float | None:
    """Compute dew point in Celsius using the Magnus approximation."""
    if temp_c is None or rh_pct is None:
        return None
    if rh_pct <= 0.0:
        return None

    a = 17.625
    b = 243.04
    gamma = math.log(rh_pct / 100.0) + (a * temp_c) / (b + temp_c)
    return (b * gamma) / (a - gamma)
