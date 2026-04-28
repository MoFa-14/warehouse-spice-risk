# File overview:
# - Responsibility: Forecast evaluation helpers.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Forecast evaluation helpers.

Responsibilities:
- Compares stored forecast trajectories against the realised 30-minute future.
- Produces numeric evidence that later supports calibration, dashboard review,
  and historical analysis.

Project flow:
- stored forecast + realised future window -> error metrics -> evaluation store

Why this matters:
- Forecast generation alone does not establish usefulness.
- These metrics create the evidence base used by later persistence comparison
  and recent-bias calibration.
"""

from __future__ import annotations

from forecasting.config import ForecastConfig
from forecasting.models import EvaluationMetrics, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import mae, mean, rmse


# Forecast scoring
# - Purpose: evaluates one stored scenario against the realised future window.
# - Project role: evaluation stage after the forecast horizon has elapsed.
# - Inputs: forecast metadata, one forecast trajectory, actual future points,
#   event metadata, and scoring thresholds.
# - Outputs: ``EvaluationMetrics`` ready for persistence and dashboard review.
# - Important decisions: scores temperature and RH directly, computes signed
#   bias for later calibration, and marks large-error windows so they can be
#   excluded from aggressive learning or calibration.
# Function purpose: Compute MAE and RMSE for one scenario.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as pod_id, ts_forecast_utc, trajectory, actual_window,
#   event_detected, config, interpreted according to the implementation below.
# - Outputs: Returns EvaluationMetrics when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def evaluate_forecast(
    *,
    pod_id: str,
    ts_forecast_utc: str,
    trajectory: ForecastTrajectory,
    actual_window: list[TimeSeriesPoint],
    event_detected: bool,
    config: ForecastConfig,
) -> EvaluationMetrics:
    """Compute MAE and RMSE for one scenario."""
    # The evaluated window is capped to the configured forecast horizon so each
    # stored metric remains comparable across forecast attempts.
    actual_temp = [point.temp_c for point in actual_window[: config.horizon_minutes]]
    actual_rh = [point.rh_pct for point in actual_window[: config.horizon_minutes]]
    mae_temp = mae(trajectory.temp_forecast_c, actual_temp)
    rmse_temp = rmse(trajectory.temp_forecast_c, actual_temp)
    mae_rh = mae(trajectory.rh_forecast_pct, actual_rh)
    rmse_rh = rmse(trajectory.rh_forecast_pct, actual_rh)
    # Signed bias preserves direction, which is required by the later automatic
    # calibration step and cannot be recovered from absolute-error metrics.
    bias_temp = mean(
        [
            float(predicted) - float(actual)
            for predicted, actual in zip(trajectory.temp_forecast_c, actual_temp)
        ]
    )
    bias_rh = mean(
        [
            float(predicted) - float(actual)
            for predicted, actual in zip(trajectory.rh_forecast_pct, actual_rh)
        ]
    )
    # The large-error flag marks windows that should contribute less trust to
    # downstream calibration and performance interpretation.
    large_error = rmse_temp > config.large_error_temp_rmse_c or rmse_rh > config.large_error_rh_rmse_pct
    notes = "large_error" if large_error else "ok"
    return EvaluationMetrics(
        ts_forecast_utc=ts_forecast_utc,
        pod_id=pod_id,
        scenario=trajectory.scenario,
        mae_temp_c=mae_temp,
        rmse_temp_c=rmse_temp,
        mae_rh_pct=mae_rh,
        rmse_rh_pct=rmse_rh,
        bias_temp_c=bias_temp,
        bias_rh_pct=bias_rh,
        event_detected=event_detected,
        large_error=large_error,
        notes=notes,
    )
