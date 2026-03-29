"""Forecast evaluation helpers."""

from __future__ import annotations

from forecasting.config import ForecastConfig
from forecasting.models import EvaluationMetrics, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import mae, rmse


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
    actual_temp = [point.temp_c for point in actual_window[: config.horizon_minutes]]
    actual_rh = [point.rh_pct for point in actual_window[: config.horizon_minutes]]
    mae_temp = mae(trajectory.temp_forecast_c, actual_temp)
    rmse_temp = rmse(trajectory.temp_forecast_c, actual_temp)
    mae_rh = mae(trajectory.rh_forecast_pct, actual_rh)
    rmse_rh = rmse(trajectory.rh_forecast_pct, actual_rh)
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
        event_detected=event_detected,
        large_error=large_error,
        notes=notes,
    )
