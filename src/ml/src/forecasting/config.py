# File overview:
# - Responsibility: Central forecasting configuration shared across ML, gateway, and
#   dashboard.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Central forecasting configuration shared across ML, gateway, and dashboard.

This file is important for viva explanation because it captures many of the
project's "research design" decisions in one place:

- the history window is fixed to 3 hours
- the forecast horizon is fixed to 30 minutes
- data is treated on a 1-minute grid
- recent-event detection uses robust thresholding rather than a hand-tuned
  fixed trigger alone
- the analogue model uses weighted similarity rather than a black-box neural
  model

When discussing why the prototype behaves the way it does, this file is the
clearest place to point to the operational assumptions and safety caps.
"""

from __future__ import annotations

from dataclasses import dataclass, field


MODEL_VERSION = "forecasting-v1"
# Class purpose: Fixed windows, thresholds, and storage-independent settings.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class ForecastConfig:
    """Fixed windows, thresholds, and storage-independent settings.

    The fields below are deliberately explicit rather than hidden inside the
    code. For a dissertation project, that makes the forecasting behaviour
    easier to justify and discuss with a supervisor:

    - window lengths define how much recent context the model sees
    - thresholds define when behaviour is considered event-like
    - rate caps stop fallback/event scenarios from becoming physically absurd
    - analogue-matching parameters explain why some historical cases are
      accepted or rejected
    """

    history_minutes: int = 180
    horizon_minutes: int = 30
    resample_minutes: int = 1
    knn_k: int = 10
    minimum_case_count: int = 5
    missing_rate_max: float = 0.10
    robust_mad_scale: float = 1.4826
    event_delta_minutes: int = 5
    event_consecutive_points: int = 2
    event_recent_minutes: int = 15
    event_threshold_multiplier: float = 3.5
    min_temp_threshold_c_5m: float = 0.25
    min_rh_threshold_pct_5m: float = 1.0
    hard_temp_jump_c_5m: float = 1.5
    hard_rh_jump_pct_5m: float = 5.0
    dew_rise_threshold_c: float = 0.2
    filter_clip_multiplier: float = 3.0
    min_temp_clip_c_per_min: float = 0.10
    min_rh_clip_pct_per_min: float = 0.40
    baseline_temp_rate_cap_c_per_min: float = 0.05
    baseline_rh_rate_cap_pct_per_min: float = 0.20
    event_temp_rate_cap_c_per_min: float = 0.30
    event_rh_rate_cap_pct_per_min: float = 1.00
    analogue_rh_gate_pct: float = 6.0
    analogue_dew_gate_c: float = 3.0
    analogue_recency_penalty_per_day: float = 0.12
    analogue_recency_penalty_cap: float = 3.0
    event_rh_decay_per_step: float = 0.90
    event_rh_max_total_drift_pct: float = 4.0
    fallback_temp_band_c: float = 0.30
    fallback_rh_band_pct: float = 1.50
    large_error_temp_rmse_c: float = 1.0
    large_error_rh_rmse_pct: float = 5.0
    feature_weights: dict[str, float] = field(
        default_factory=lambda: {
            "temp_last": 1.0,
            "rh_last": 1.0,
            "dew_last": 0.8,
            "temp_slope_15": 1.2,
            "temp_slope_30": 1.4,
            "temp_slope_60": 1.1,
            "rh_slope_15": 1.2,
            "rh_slope_30": 1.4,
            "rh_slope_60": 1.1,
            "dew_slope_30": 0.8,
            "temp_std_30": 0.8,
            "temp_std_60": 0.7,
            "rh_std_30": 0.8,
            "rh_std_60": 0.7,
            "temp_min_60": 0.7,
            "temp_max_60": 0.7,
            "rh_min_60": 0.7,
            "rh_max_60": 0.7,
            "hour_sin": 0.9,
            "hour_cos": 0.9,
        }
    )
# Function purpose: Build a validated config from CLI-level overrides.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as k, missing_rate_max, history_minutes, horizon_minutes,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns ForecastConfig when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def build_config(
    *,
    k: int | None = None,
    missing_rate_max: float | None = None,
    history_minutes: int = 180,
    horizon_minutes: int = 30,
) -> ForecastConfig:
    """Build a validated config from CLI-level overrides.

    In project terms, this is the small guardrail between "research design"
    decisions and "runtime control" decisions.

    The runner is allowed to change a small number of operational settings, such
    as the neighbour count or missing-data tolerance, but it is *not* allowed to
    silently change the core dissertation assumptions like the 3-hour history
    window. That is why the function validates those values here instead of
    trusting every caller.
    """
    if history_minutes != 180:
        raise ValueError("history_minutes is fixed to 180 for this forecaster.")
    if horizon_minutes <= 0:
        raise ValueError("horizon_minutes must be greater than 0.")

    defaults = ForecastConfig()
    requested_k = defaults.knn_k if k is None else int(k)
    return ForecastConfig(
        history_minutes=history_minutes,
        horizon_minutes=horizon_minutes,
        knn_k=requested_k,
        minimum_case_count=min(requested_k, defaults.minimum_case_count),
        missing_rate_max=defaults.missing_rate_max if missing_rate_max is None else float(missing_rate_max),
    )
