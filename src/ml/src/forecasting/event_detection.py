"""Robust event detection over the recent portion of the 3-hour history window.

In this project, forecasting is not treated as one single mode of operation.
The system first asks a practical warehouse question:

"Do the latest readings still look like ordinary storage behaviour, or do they
look like a disturbance that should be interpreted differently?"

This file answers that question using robust 5-minute changes in temperature,
RH, and dew point. The output is later used by the runner to decide whether it
should produce only the normal baseline forecast or also generate an
event-persist scenario.
"""

from __future__ import annotations

from statistics import median

from forecasting.config import ForecastConfig
from forecasting.models import EventDetectionResult, TimeSeriesPoint
from forecasting.utils import median_absolute_deviation, pairwise_differences


def detect_recent_event(
    window: list[TimeSeriesPoint],
    *,
    config: ForecastConfig,
) -> EventDetectionResult:
    """Detect whether the most recent part of the window contains a disturbance.

    Inputs:
    - the full 3-hour resampled telemetry window
    - configuration values that define robust thresholds and recent-event rules

    Output:
    - an ``EventDetectionResult`` describing whether the recent behaviour looks
      event-like and, if so, what kind of event signature it most resembles

    In viva terms, this is the point where the prototype decides whether to
    trust the recent history as a normal storage baseline or to treat the tail
    of the window as a disturbance.
    """
    if len(window) < config.event_delta_minutes + config.event_consecutive_points + 1:
        return EventDetectionResult(
            event_detected=False,
            event_type="none",
            event_reason="not_enough_history",
        )

    temps = [point.temp_c for point in window]
    rhs = [point.rh_pct for point in window]
    dews = [point.dew_point_c for point in window]

    # We compare readings separated by a fixed 5-minute gap because warehouse
    # disturbances are easier to interpret as short-window changes than as
    # minute-to-minute noise.
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

    # "Breach" flags mark where the observed 5-minute changes are unusually
    # large relative to the recent baseline behaviour.
    temp_breaches = [abs(value) > temp_threshold for value in temp_deltas]
    rh_breaches = [abs(value) > rh_threshold for value in rh_deltas]
    hard_temp_breaches = [abs(value) > config.hard_temp_jump_c_5m for value in temp_deltas]
    hard_rh_breaches = [abs(value) > config.hard_rh_jump_pct_5m for value in rh_deltas]

    # The system only cares about disturbances near "now". Older disturbances
    # are part of the historical context, not part of the current operating
    # state that should trigger an alternate scenario.
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
    # Dew-point change is used as a physically meaningful cross-check. For
    # example, a humidity-driven disturbance is more believable when dew point
    # also rises rather than when RH changes alone are caused only by
    # temperature.
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


def _robust_threshold(values: list[float], *, multiplier: float, scale: float, minimum: float) -> float:
    """Build a robust change threshold from the recent distribution of deltas.

    The threshold is based on median absolute deviation rather than variance so
    one or two strong disturbances do not completely dominate the threshold used
    to detect the next one.
    """
    if not values:
        return minimum
    center = median(values)
    mad = median_absolute_deviation(values) * scale
    if mad == 0.0:
        dispersion = max(minimum, max(abs(value - center) for value in values))
    else:
        dispersion = multiplier * mad
    return max(minimum, dispersion)


def _latest_breach_segment(
    *,
    temp_breaches: list[bool],
    rh_breaches: list[bool],
    hard_temp_breaches: list[bool],
    hard_rh_breaches: list[bool],
    recent_start: int,
    consecutive_points: int,
) -> tuple[int, int, int] | None:
    """Return the most recent consecutive breach segment, if one exists.

    The forecast runner only needs the latest event-like section, because that
    is the part of the window that influences whether an event-persist scenario
    should be generated right now.
    """
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


def _event_type(
    *,
    temp_delta: float,
    temp_threshold: float,
    rh_delta: float,
    rh_threshold: float,
    dew_rise: float,
    config: ForecastConfig,
) -> str:
    """Translate raw change magnitudes into a simple project-facing event label.

    The labels here are intentionally lightweight. They are not claiming to
    identify the real physical cause with certainty; they are providing a
    human-readable explanation for the dashboard and for viva discussion.
    """
    temp_ratio = abs(temp_delta) / max(temp_threshold, 1e-6)
    rh_ratio = abs(rh_delta) / max(rh_threshold, 1e-6)
    rh_absolute_dominance = abs(rh_delta) >= config.hard_rh_jump_pct_5m and abs(rh_delta) >= max(3.0 * abs(temp_delta), 3.0)

    if (rh_ratio >= temp_ratio or rh_absolute_dominance) and dew_rise >= config.dew_rise_threshold_c:
        return "door_open_like"
    if temp_ratio > rh_ratio and abs(rh_delta) < max(config.min_rh_threshold_pct_5m, config.hard_rh_jump_pct_5m / 2.0):
        return "ventilation_issue_like"
    return "unknown"
