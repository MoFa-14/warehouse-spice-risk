"""Forecast evaluation helpers.

This file turns stored forecasts and later observed reality into numerical
evidence. In the dissertation context, it is the bridge between "the model made
a prediction" and "we can now judge whether that prediction was useful".
"""

from __future__ import annotations

from forecasting.config import ForecastConfig
from forecasting.models import EvaluationMetrics, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import mae, mean, rmse


def evaluate_forecast(
    *,
    pod_id: str,
    ts_forecast_utc: str,
    trajectory: ForecastTrajectory,
    actual_window: list[TimeSeriesPoint],
    event_detected: bool,
    config: ForecastConfig,
) -> EvaluationMetrics:
    """Compute MAE and RMSE for one scenario.

    Inputs:
    - one forecast trajectory
    - the realised 30-minute future window

    Output:
    - an ``EvaluationMetrics`` record that can be stored, analysed later, and
      compared against persistence in the dashboard

    The prototype currently scores temperature and RH directly. Dew point is not
    evaluated here as a first-class stored metric because it is derived from the
    forecasted temperature and RH paths.
    """
    # The evaluation horizon is capped explicitly so the metric always reflects
    # the dissertation's fixed 30-minute prediction task.
    actual_temp = [point.temp_c for point in actual_window[: config.horizon_minutes]]
    actual_rh = [point.rh_pct for point in actual_window[: config.horizon_minutes]]
    mae_temp = mae(trajectory.temp_forecast_c, actual_temp)
    rmse_temp = rmse(trajectory.temp_forecast_c, actual_temp)
    mae_rh = mae(trajectory.rh_forecast_pct, actual_rh)
    rmse_rh = rmse(trajectory.rh_forecast_pct, actual_rh)
    # Signed bias is useful for later automatic calibration because it tells us
    # whether the model tends to sit above or below reality, not just how far
    # away it is.
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
    # "Large error" is a project-facing quality flag. Later stages use it to
    # avoid learning too aggressively from obviously poor windows.
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
