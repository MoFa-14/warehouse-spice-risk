# File overview:
# - Responsibility: Recent-event detection for the forecasting pipeline.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Recent-event detection for the forecasting pipeline.

Responsibilities:
- Inspects the latest part of the 3-hour history window for disturbance-like
  behaviour.
- Uses robust 5-minute change statistics rather than single-minute noise.
- Produces the event label and segment metadata used by both baseline filtering
  and alternate scenario generation.

Project flow:
- history window -> event detection -> baseline filtering + scenario selection

Why this matters:
- The forecasting subsystem operates in two modes: ordinary baseline
  continuation and disturbance-aware interpretation.
- Detecting the difference keeps analogue matching focused on normal behaviour
  while still allowing an alternate event-persist scenario when recent changes
  look operationally significant.
"""

from __future__ import annotations

from statistics import median

from forecasting.config import ForecastConfig
from forecasting.models import EventDetectionResult, TimeSeriesPoint
from forecasting.utils import median_absolute_deviation, pairwise_differences


# Recent-event decision
# - Purpose: decides whether the tail of the current history window should be
#   treated as an operational disturbance.
# - Project role: decision stage between telemetry preparation and forecast
#   scenario selection.
# - Inputs: the full 3-hour resampled window plus threshold configuration.
# - Outputs: ``EventDetectionResult`` with event flag, label, thresholds, and
#   the relevant segment boundaries.
# - Important decisions: compares robust 5-minute changes, restricts attention
#   to the most recent portion of the window, and uses dew-point behaviour as a
#   cross-check for humidity-led events.
# - Related flow: used by ``build_baseline_window`` and
#   ``build_event_persist_forecast``.
# Function purpose: Detect whether the most recent part of the window contains a
#   disturbance.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as window, config, interpreted according to the
#   implementation below.
# - Outputs: Returns EventDetectionResult when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def detect_recent_event(
    window: list[TimeSeriesPoint],
    *,
    config: ForecastConfig,
) -> EventDetectionResult:
    """Detect whether the most recent part of the window contains a disturbance."""
    if len(window) < config.event_delta_minutes + config.event_consecutive_points + 1:
        return EventDetectionResult(
            event_detected=False,
            event_type="none",
            event_reason="not_enough_history",
        )

    temps = [point.temp_c for point in window]
    rhs = [point.rh_pct for point in window]
    dews = [point.dew_point_c for point in window]

    # Change extraction
    # Fixed 5-minute deltas suppress minute-to-minute noise and make the recent
    # disturbance signal easier to interpret in operational terms.
    temp_deltas = pairwise_differences(temps, config.event_delta_minutes)
    rh_deltas = pairwise_differences(rhs, config.event_delta_minutes)
    temp_threshold = _robust_threshold(
        temp_deltas,
        multiplier=config.event_threshold_multiplier,
        scale=config.robust_mad_scale,
        minimum=config.min_temp_threshold_c_5m,
    )
    rh_threshold = _robust_threshold(
        rh_deltas,
        multiplier=config.event_threshold_multiplier,
        scale=config.robust_mad_scale,
        minimum=config.min_rh_threshold_pct_5m,
    )

    # Threshold application
    # Breach flags mark points where recent change magnitudes sit outside the
    # robust baseline envelope derived from the same window.
    temp_breaches = [abs(value) > temp_threshold for value in temp_deltas]
    rh_breaches = [abs(value) > rh_threshold for value in rh_deltas]
    hard_temp_breaches = [abs(value) > config.hard_temp_jump_c_5m for value in temp_deltas]
    hard_rh_breaches = [abs(value) > config.hard_rh_jump_pct_5m for value in rh_deltas]

    # Recent-tail focus
    # Older disturbances remain part of the historical context, but only the
    # latest segment should influence the current forecast mode decision.
    recent_start = max(0, len(temp_deltas) - config.event_recent_minutes)
    candidate = _latest_breach_segment(
        temp_breaches=temp_breaches,
        rh_breaches=rh_breaches,
        hard_temp_breaches=hard_temp_breaches,
        hard_rh_breaches=hard_rh_breaches,
        recent_start=recent_start,
        consecutive_points=config.event_consecutive_points,
    )
    if candidate is None:
        return EventDetectionResult(
            event_detected=False,
            event_type="none",
            event_reason=(
                f"no_recent_event temp_thr_5m={temp_threshold:.2f}C "
                f"rh_thr_5m={rh_threshold:.2f}%"
            ),
            temp_threshold_c_5m=temp_threshold,
            rh_threshold_pct_5m=rh_threshold,
        )

    segment_start_delta, segment_end_delta, consecutive_points = candidate
    end_point_index = segment_end_delta + config.event_delta_minutes
    start_point_index = segment_start_delta
    temp_delta = temp_deltas[segment_end_delta]
    rh_delta = rh_deltas[segment_end_delta]
    dew_reference_index = max(0, end_point_index - 15)
    # Physical consistency check
    # Dew-point rise distinguishes genuine moisture-led changes from RH swings
    # caused mainly by temperature movement alone.
    dew_rise = dews[end_point_index] - dews[dew_reference_index]

    event_type = _event_type(
        temp_delta=temp_delta,
        temp_threshold=temp_threshold,
        rh_delta=rh_delta,
        rh_threshold=rh_threshold,
        dew_rise=dew_rise,
        config=config,
    )
    reason = (
        f"dT5={temp_delta:.2f}C thrT={temp_threshold:.2f}C "
        f"dRH5={rh_delta:.2f}% thrRH={rh_threshold:.2f}% "
        f"dew15={dew_rise:.2f}C run={consecutive_points}"
    )

    return EventDetectionResult(
        event_detected=True,
        event_type=event_type,
        event_reason=reason,
        segment_start_index=start_point_index,
        segment_end_index=end_point_index,
        temp_threshold_c_5m=temp_threshold,
        rh_threshold_pct_5m=rh_threshold,
        temp_delta_c_5m=temp_delta,
        rh_delta_pct_5m=rh_delta,
        consecutive_points=consecutive_points,
    )


# Robust threshold builder
# - Purpose: derives a disturbance threshold from the local distribution of
#   recent deltas.
# - Design reason: median absolute deviation is used so one already-large event
#   does not inflate the threshold enough to hide the next one.
# Function purpose: Build a robust change threshold from the recent distribution of
#   deltas.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, multiplier, scale, minimum, interpreted
#   according to the implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _robust_threshold(values: list[float], *, multiplier: float, scale: float, minimum: float) -> float:
    """Build a robust change threshold from the recent distribution of deltas."""
    if not values:
        return minimum
    center = median(values)
    mad = median_absolute_deviation(values) * scale
    if mad == 0.0:
        dispersion = max(minimum, max(abs(value - center) for value in values))
    else:
        dispersion = multiplier * mad
    return max(minimum, dispersion)


# Recent breach segmentation
# - Purpose: finds the latest contiguous stretch of disturbance evidence.
# - Project role: translates point-wise threshold breaches into one actionable
#   recent segment for downstream filtering and alternate-scenario decisions.
# - Important decisions: hard breaches can trigger an immediate one-point event;
#   softer breaches require a configured consecutive run length.
# Function purpose: Return the most recent consecutive breach segment, if one
#   exists.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as temp_breaches, rh_breaches, hard_temp_breaches,
#   hard_rh_breaches, recent_start, consecutive_points, interpreted according to the
#   implementation below.
# - Outputs: Returns tuple[int, int, int] | None when the function completes
#   successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _latest_breach_segment(
    *,
    temp_breaches: list[bool],
    rh_breaches: list[bool],
    hard_temp_breaches: list[bool],
    hard_rh_breaches: list[bool],
    recent_start: int,
    consecutive_points: int,
) -> tuple[int, int, int] | None:
    """Return the most recent consecutive breach segment, if one exists."""
    latest: tuple[int, int, int] | None = None
    current_start: int | None = None
    current_length = 0

    for index in range(recent_start, len(temp_breaches)):
        combined = temp_breaches[index] or rh_breaches[index]
        hard = hard_temp_breaches[index] or hard_rh_breaches[index]
        if combined or hard:
            current_start = index if current_start is None else current_start
            current_length += 1
        else:
            if current_start is not None and current_length >= consecutive_points:
                latest = (current_start, index - 1, current_length)
            current_start = None
            current_length = 0

        if hard:
            latest = (index, index, 1)

    if current_start is not None and current_length >= consecutive_points:
        latest = (current_start, len(temp_breaches) - 1, current_length)
    return latest


# Event labelling
# - Purpose: converts thresholded change evidence into a compact operational
#   label.
# - Outputs: ``door_open_like``, ``ventilation_issue_like``, or ``unknown``.
# - Important assumption: labels describe the observed pattern, not a proven
#   physical cause.
# Function purpose: Translate raw change magnitudes into a simple project-facing
#   event label.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as temp_delta, temp_threshold, rh_delta, rh_threshold,
#   dew_rise, config, interpreted according to the implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _event_type(
    *,
    temp_delta: float,
    temp_threshold: float,
    rh_delta: float,
    rh_threshold: float,
    dew_rise: float,
    config: ForecastConfig,
) -> str:
    """Translate raw change magnitudes into a simple project-facing event label."""
    temp_ratio = abs(temp_delta) / max(temp_threshold, 1e-6)
    rh_ratio = abs(rh_delta) / max(rh_threshold, 1e-6)
    rh_absolute_dominance = abs(rh_delta) >= config.hard_rh_jump_pct_5m and abs(rh_delta) >= max(3.0 * abs(temp_delta), 3.0)

    if (rh_ratio >= temp_ratio or rh_absolute_dominance) and dew_rise >= config.dew_rise_threshold_c:
        return "door_open_like"
    if temp_ratio > rh_ratio and abs(rh_delta) < max(config.min_rh_threshold_pct_5m, config.hard_rh_jump_pct_5m / 2.0):
        return "ventilation_issue_like"
    return "unknown"
