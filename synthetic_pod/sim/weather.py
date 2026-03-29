"""Lightweight Bristol-inspired weather references for indoor warehouse trends."""

from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import datetime


MONTHLY_OUTDOOR_TEMP_C = (
    6.2,
    6.8,
    8.9,
    11.2,
    14.1,
    16.8,
    18.8,
    18.7,
    16.6,
    12.9,
    9.1,
    6.8,
)
MONTHLY_OUTDOOR_RH_PCT = (
    86.0,
    84.0,
    81.0,
    78.0,
    76.0,
    75.0,
    76.0,
    77.0,
    80.0,
    83.0,
    85.0,
    86.0,
)
MONTHLY_OUTDOOR_TEMP_SWING_C = (
    2.6,
    2.8,
    3.3,
    4.0,
    4.5,
    5.0,
    5.2,
    5.0,
    4.4,
    3.7,
    3.0,
    2.7,
)
MONTHLY_OUTDOOR_RH_SWING_PCT = (
    4.0,
    4.0,
    4.5,
    5.0,
    5.5,
    6.0,
    6.0,
    5.5,
    5.0,
    4.5,
    4.0,
    4.0,
)

ANNUAL_OUTDOOR_TEMP_MEAN_C = sum(MONTHLY_OUTDOOR_TEMP_C) / len(MONTHLY_OUTDOOR_TEMP_C)
ANNUAL_OUTDOOR_RH_MEAN_PCT = sum(MONTHLY_OUTDOOR_RH_PCT) / len(MONTHLY_OUTDOOR_RH_PCT)


@dataclass(frozen=True)
class IndoorClimateTarget:
    """Indoor warehouse target plus the outdoor reference used to shape it."""

    indoor_temp_c: float
    indoor_rh_pct: float
    outdoor_temp_c: float
    outdoor_rh_pct: float


def bristol_indoor_target(
    when_local: datetime,
    *,
    base_temp_c: float,
    base_rh_pct: float,
    seasonal_temp_weight: float,
    seasonal_rh_weight: float,
    diurnal_temp_weight: float,
    diurnal_rh_weight: float,
) -> IndoorClimateTarget:
    """Blend a warehouse baseline with a damped Bristol-like seasonal and day-night trend."""
    month_temp_c = _interpolate_monthly(MONTHLY_OUTDOOR_TEMP_C, when_local)
    month_rh_pct = _interpolate_monthly(MONTHLY_OUTDOOR_RH_PCT, when_local)
    temp_swing_c = _interpolate_monthly(MONTHLY_OUTDOOR_TEMP_SWING_C, when_local)
    rh_swing_pct = _interpolate_monthly(MONTHLY_OUTDOOR_RH_SWING_PCT, when_local)

    hour = when_local.hour + (when_local.minute / 60.0) + (when_local.second / 3600.0)
    temp_wave = math.sin((2.0 * math.pi * (hour - 9.0)) / 24.0)
    rh_wave = -0.8 * temp_wave

    seasonal_temp_offset_c = month_temp_c - ANNUAL_OUTDOOR_TEMP_MEAN_C
    seasonal_rh_offset_pct = month_rh_pct - ANNUAL_OUTDOOR_RH_MEAN_PCT
    outdoor_temp_c = month_temp_c + (temp_swing_c * temp_wave)
    outdoor_rh_pct = month_rh_pct + (rh_swing_pct * rh_wave)

    indoor_temp_c = (
        base_temp_c
        + (seasonal_temp_weight * seasonal_temp_offset_c)
        + (diurnal_temp_weight * temp_swing_c * temp_wave)
    )
    indoor_rh_pct = (
        base_rh_pct
        + (seasonal_rh_weight * seasonal_rh_offset_pct)
        + (diurnal_rh_weight * rh_swing_pct * rh_wave)
    )

    return IndoorClimateTarget(
        indoor_temp_c=indoor_temp_c,
        indoor_rh_pct=indoor_rh_pct,
        outdoor_temp_c=outdoor_temp_c,
        outdoor_rh_pct=outdoor_rh_pct,
    )


def _interpolate_monthly(values: tuple[float, ...], when_local: datetime) -> float:
    current_index = when_local.month - 1
    next_index = (current_index + 1) % len(values)
    days_in_month = calendar.monthrange(when_local.year, when_local.month)[1]
    fraction = (
        (when_local.day - 1)
        + (when_local.hour / 24.0)
        + (when_local.minute / 1440.0)
        + (when_local.second / 86400.0)
    ) / days_in_month
    return ((1.0 - fraction) * values[current_index]) + (fraction * values[next_index])
