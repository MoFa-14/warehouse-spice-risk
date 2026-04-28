# File overview:
# - Responsibility: Shared forecasting data structures.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Shared forecasting data structures.

These dataclasses are the common language of the forecasting subsystem.
Showing this file to a supervisor is useful because it makes the pipeline
readable as a sequence of named project concepts rather than anonymous
dictionaries:

- ``TimeSeriesPoint``: one minute of cleaned telemetry
- ``EventDetectionResult``: the decision about whether recent behaviour looks
  normal or disturbance-like
- ``FeatureVector``: the condensed summary of the 3-hour window used for
  analogue matching
- ``ForecastTrajectory``: one scenario's forward path
- ``ForecastBundle``: the full forecast package saved by the gateway
- ``CaseRecord``: one historical example used by the analogue model
- ``EvaluationMetrics``: the scorecard produced once actual future readings are
  available
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
# Class purpose: One resampled 1-minute telemetry point used by the forecaster.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class TimeSeriesPoint:
    """One resampled 1-minute telemetry point used by the forecaster.

    By the time data reaches this structure, it has already moved beyond raw
    sensor packets. The point now represents a minute-level "best estimate" of
    warehouse conditions, including whether the value was genuinely observed or
    filled/interpolated during resampling.
    """

    ts_utc: datetime
    temp_c: float
    rh_pct: float
    dew_point_c: float
    observed: bool = True
# Class purpose: Outcome of recent-event detection over the last 3 hours.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class EventDetectionResult:
    """Outcome of recent-event detection over the last 3 hours.

    In viva terms, this is the point where the system decides whether the
    recent telemetry still looks like normal storage behaviour or whether it
    resembles a disturbance such as a door opening or ventilation change.
    """

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
# Class purpose: Named numerical features used for analogue similarity matching.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class FeatureVector:
    """Named numerical features used for analogue similarity matching.

    This structure is the bridge between raw time series and case-based
    reasoning. It keeps the current forecast query compact enough to compare
    against many historical cases while still retaining the main physical
    context: level, trend, variability, and time-of-day.
    """

    ts_pc_utc: str
    values: dict[str, float]
    missing_rate: float
    observed_points: int
# Class purpose: One point forecast scenario plus uncertainty bands.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class ForecastTrajectory:
    """One point forecast scenario plus uncertainty bands.

    The project stores separate trajectories for baseline and event-persist
    scenarios. Each trajectory carries both the main path and an uncertainty
    envelope so the dashboard can show the forecast as more than a single line.
    """

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
# Class purpose: Full forecasting output for one pod and one forecast timestamp.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class ForecastBundle:
    """Full forecasting output for one pod and one forecast timestamp.

    This is the object the gateway saves after each forecast cycle. It bundles
    together the forecast timestamp, the event-detection decision, the feature
    vector used for matching, and the resulting scenario trajectories so that
    the dashboard can later explain not only *what* was predicted but also the
    context in which the prediction was made.
    """

    pod_id: str
    ts_pc_utc: str
    model_version: str
    missing_rate: float
    event: EventDetectionResult
    feature_vector: FeatureVector
    baseline: ForecastTrajectory
    event_persist: ForecastTrajectory | None = None
    metadata: dict[str, str | int | float] = field(default_factory=dict)
# Class purpose: Persisted analogue case with features and the realized 30-minute
#   future.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class CaseRecord:
    """Persisted analogue case with features and the realized 30-minute future.

    A case is effectively one historical "question + answer" pair:
    - the feature vector is the question: what did the previous 3 hours look
      like?
    - the future trajectories are the answer: what actually happened in the
      next 30 minutes?
    """

    ts_pc_utc: str
    pod_id: str
    feature_vector: dict[str, float]
    future_temp_c: list[float]
    future_rh_pct: list[float]
    event_label: str = "none"
# Class purpose: Forecast error summary for one scenario over the 30-minute horizon.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass(frozen=True)
class EvaluationMetrics:
    """Forecast error summary for one scenario over the 30-minute horizon.

    The dashboard and later analysis work rely on this object to compare:
    - model vs actual behaviour
    - model vs persistence baseline
    - normal windows vs event-like windows

    It therefore acts as the project’s retrospective evidence layer rather than
    the live forecasting layer.
    """

    ts_forecast_utc: str
    pod_id: str
    scenario: str
    mae_temp_c: float
    rmse_temp_c: float
    mae_rh_pct: float
    rmse_rh_pct: float
    bias_temp_c: float
    bias_rh_pct: float
    event_detected: bool
    large_error: bool
    persistence_mae_temp_c: float | None = None
    persistence_rmse_temp_c: float | None = None
    persistence_mae_rh_pct: float | None = None
    persistence_rmse_rh_pct: float | None = None
    notes: str = ""
