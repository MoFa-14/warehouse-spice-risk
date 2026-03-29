"""Central configuration for the lightweight forecasting pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


MODEL_VERSION = "forecasting-v1"


@dataclass(frozen=True)
class ForecastConfig:
    """Fixed windows, thresholds, and storage-independent settings."""

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


def build_config(
    *,
    k: int | None = None,
    missing_rate_max: float | None = None,
    history_minutes: int = 180,
    horizon_minutes: int = 30,
) -> ForecastConfig:
    """Build a validated config from CLI-level overrides."""
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
