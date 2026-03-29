"""Event-robust filtering helpers for baseline forecasting."""

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
    """Return an event-robust baseline series while keeping raw input untouched."""
    if not detection.event_detected or detection.segment_start_index is None:
        return list(window)

    temp_values = [point.temp_c for point in window]
    rh_values = [point.rh_pct for point in window]
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
    if len(values) < 2 or start_index <= 0:
        return list(values)

    raw_diffs = pairwise_differences(values, 1)
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
            raw_delta = clamp(raw_delta, lower, upper)
        filtered.append(filtered[-1] + raw_delta)
    return filtered
