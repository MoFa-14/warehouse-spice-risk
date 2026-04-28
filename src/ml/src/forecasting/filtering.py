# File overview:
# - Responsibility: Event-robust filtering helpers for baseline forecasting.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Event-robust filtering helpers for baseline forecasting.

Responsibilities:
- Produces a baseline-safe version of the recent history window after event
  detection has identified a disturbance.
- Preserves the raw window for event-aware analysis while softening the tail
  used for baseline analogue matching.

Project flow:
- history window -> recent-event detection -> baseline-window filtering ->
  feature extraction -> baseline analogue forecast

Design reason:
- The baseline forecast should reflect normal continuation rather than the
  immediate continuation of a transient disturbance.
"""

from __future__ import annotations

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import EventDetectionResult, TimeSeriesPoint
from forecasting.utils import clamp, mean, median_absolute_deviation, pairwise_differences


# Baseline-window preparation
# - Purpose: returns the filtered history window used for baseline feature
#   extraction and analogue matching.
# - Project role: preprocessing stage between event detection and feature
#   extraction.
# - Inputs: raw 3-hour history window, disturbance metadata, and filtering
#   thresholds.
# - Outputs: a ``TimeSeriesPoint`` list with temperature, RH, and dew point kept
#   physically consistent.
# - Important decision: passes raw data through unchanged when no recent event
#   exists, but clips post-event slopes when the tail is judged distortionary.
# Function purpose: Return an event-robust baseline series while keeping raw input
#   untouched.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as window, detection, config, interpreted according to
#   the implementation below.
# - Outputs: Returns list[TimeSeriesPoint] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

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
    # Temperature and RH are clipped independently, then dew point is
    # recomputed so the filtered baseline remains psychrometrically consistent.
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


# Post-event slope clipping
# - Purpose: limits minute-to-minute changes after the event start to the range
#   implied by pre-event behaviour.
# - Project role: protects baseline matching from a distorted tail without
#   discarding the rest of the window.
# - Important decision: the acceptable delta band is estimated only from
#   pre-event movement, so the disturbance does not define its own clipping
#   threshold.
# Function purpose: Clip post-event minute-to-minute deltas using a robust pre-event
#   range.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, start_index, minimum_clip, clip_multiplier,
#   interpreted according to the implementation below.
# - Outputs: Returns list[float] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _clip_from_index(
    values: list[float],
    *,
    start_index: int,
    minimum_clip: float,
    clip_multiplier: float,
) -> list[float]:
    """Clip post-event minute-to-minute deltas using a robust pre-event range."""
    if len(values) < 2 or start_index <= 0:
        return list(values)

    raw_diffs = pairwise_differences(values, 1)
    # Only pre-event movement is allowed to define the normal delta envelope.
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
            # Once inside the detected disturbance region, deltas are constrained
            # back into the pre-event range so baseline matching does not treat a
            # short disruption as the new normal.
            raw_delta = clamp(raw_delta, lower, upper)
        filtered.append(filtered[-1] + raw_delta)
    return filtered
