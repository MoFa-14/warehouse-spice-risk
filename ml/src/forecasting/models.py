"""Shared forecasting data structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class TimeSeriesPoint:
    """One resampled 1-minute telemetry point used by the forecaster."""

    ts_utc: datetime
    temp_c: float
    rh_pct: float
    dew_point_c: float
    observed: bool = True


@dataclass(frozen=True)
class EventDetectionResult:
    """Outcome of recent-event detection over the last 3 hours."""

    event_detected: bool
    event_type: str
    event_reason: str
    segment_start_index: int | None = None
    segment_end_index: int | None = None
    temp_threshold_c_5m: float = 0.0
    rh_threshold_pct_5m: float = 0.0
    temp_delta_c_5m: float = 0.0
    rh_delta_pct_5m: float = 0.0
    consecutive_points: int = 0


@dataclass(frozen=True)
class FeatureVector:
    """Named numerical features used for analogue similarity matching."""

    ts_pc_utc: str
    values: dict[str, float]
    missing_rate: float
    observed_points: int


@dataclass(frozen=True)
class ForecastTrajectory:
    """One point forecast scenario plus uncertainty bands."""

    scenario: str
    temp_forecast_c: list[float]
    rh_forecast_pct: list[float]
    dew_point_forecast_c: list[float]
    temp_p25_c: list[float]
    temp_p75_c: list[float]
    rh_p25_pct: list[float]
    rh_p75_pct: list[float]
    source: str
    neighbor_count: int
    case_count: int
    notes: str = ""


@dataclass(frozen=True)
class ForecastBundle:
    """Full forecasting output for one pod and one forecast timestamp."""

    pod_id: str
    ts_pc_utc: str
    model_version: str
    missing_rate: float
    event: EventDetectionResult
    feature_vector: FeatureVector
    baseline: ForecastTrajectory
    event_persist: ForecastTrajectory | None = None
    metadata: dict[str, str | int | float] = field(default_factory=dict)


@dataclass(frozen=True)
class CaseRecord:
    """Persisted analogue case with features and the realized 30-minute future."""

    ts_pc_utc: str
    pod_id: str
    feature_vector: dict[str, float]
    future_temp_c: list[float]
    future_rh_pct: list[float]
    event_label: str = "none"


@dataclass(frozen=True)
class EvaluationMetrics:
    """Forecast error summary for one scenario over the 30-minute horizon."""

    ts_forecast_utc: str
    pod_id: str
    scenario: str
    mae_temp_c: float
    rmse_temp_c: float
    mae_rh_pct: float
    rmse_rh_pct: float
    event_detected: bool
    large_error: bool
    notes: str = ""
