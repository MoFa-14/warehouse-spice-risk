"""Event-robust filtering helpers for baseline forecasting.

The forecasting pipeline deliberately separates two ideas:
- what *actually happened* in the recent readings
- what should be treated as the stable baseline for analogue matching

If a short disturbance has just happened, the raw history still matters for the
event-persist scenario, but the baseline forecaster should not be forced to
match historical cases against a distorted tail. This file therefore creates an
event-robust version of the recent history without modifying the raw input
itself.
"""

from __future__ import annotations

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import EventDetectionResult, TimeSeriesPoint
from forecasting.utils import clamp, mean, median_absolute_deviation, pairwise_differences


def build_baseline_window(
    window: list[TimeSeriesPoint],
    *,
    detection: EventDetectionResult,
    config: ForecastConfig,
) -> list[TimeSeriesPoint]:
    """Return an event-robust baseline series while keeping raw input untouched.

    This function is called after event detection and before feature extraction.
    If no recent event is detected, it simply passes the original window
    through. If an event *is* detected, it clips the extreme post-event slopes
    so the baseline forecast represents "normal continuation" rather than
    "continuation of a disturbance".
    """
    if not detection.event_detected or detection.segment_start_index is None:
        return list(window)

    temp_values = [point.temp_c for point in window]
    rh_values = [point.rh_pct for point in window]
    # Temperature and RH are filtered separately, then dew point is recomputed
    # from the filtered values so the three variables remain physically
    # consistent.
    filtered_temp = _clip_from_index(
        temp_values,
        start_index=detection.segment_start_index,
        minimum_clip=config.min_temp_clip_c_per_min,
        clip_multiplier=config.filter_clip_multiplier,
    )
    filtered_rh = _clip_from_index(
        rh_values,
        start_index=detection.segment_start_index,
        minimum_clip=config.min_rh_clip_pct_per_min,
        clip_multiplier=config.filter_clip_multiplier,
    )

    filtered: list[TimeSeriesPoint] = []
    for index, point in enumerate(window):
        filtered.append(
            TimeSeriesPoint(
                ts_utc=point.ts_utc,
                temp_c=filtered_temp[index],
                rh_pct=filtered_rh[index],
                dew_point_c=calculate_dew_point_c(filtered_temp[index], filtered_rh[index]),
                observed=point.observed,
            )
        )
    return filtered


def _clip_from_index(
    values: list[float],
    *,
    start_index: int,
    minimum_clip: float,
    clip_multiplier: float,
) -> list[float]:
    """Clip post-event minute-to-minute deltas using a robust pre-event range.

    In plain language, once the event start is known, we estimate what "normal"
    minute-level change looked like before that event. Any much larger change
    after the event start is limited back into that normal band.
    """
    if len(values) < 2 or start_index <= 0:
        return list(values)

    raw_diffs = pairwise_differences(values, 1)
    # Only pre-event behaviour is used to define the acceptable slope range.
    baseline_diffs = raw_diffs[: max(1, start_index - 1)]
    center = mean(baseline_diffs)
    dispersion = median_absolute_deviation(baseline_diffs) * 1.4826 * clip_multiplier
    limit = max(minimum_clip, dispersion)
    lower = center - limit
    upper = center + limit

    filtered = [float(values[0])]
    for index in range(1, len(values)):
        raw_delta = float(values[index]) - float(values[index - 1])
        if index >= start_index:
            # After the event begins, the delta is constrained into the normal
            # range so the baseline window does not overreact to the disturbance.
            raw_delta = clamp(raw_delta, lower, upper)
        filtered.append(filtered[-1] + raw_delta)
    return filtered
