from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from _helpers import ML_SRC  # noqa: F401
from forecasting.config import build_config
from forecasting.evaluator import evaluate_forecast
from forecasting.models import ForecastTrajectory, TimeSeriesPoint


class EvaluatorTests(unittest.TestCase):
    def test_mae_and_rmse_are_computed_correctly(self) -> None:
        start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
        actual = [
            TimeSeriesPoint(start + timedelta(minutes=1), temp_c=21.0, rh_pct=51.0, dew_point_c=10.0),
            TimeSeriesPoint(start + timedelta(minutes=2), temp_c=21.0, rh_pct=49.0, dew_point_c=10.0),
        ]
        trajectory = ForecastTrajectory(
            scenario="baseline",
            temp_forecast_c=[20.0, 22.0],
            rh_forecast_pct=[50.0, 50.0],
            dew_point_forecast_c=[10.0, 10.0],
            temp_p25_c=[19.0, 21.0],
            temp_p75_c=[21.0, 23.0],
            rh_p25_pct=[49.0, 49.0],
            rh_p75_pct=[51.0, 51.0],
            source="test",
            neighbor_count=0,
            case_count=0,
        )

        metrics = evaluate_forecast(
            pod_id="01",
            ts_forecast_utc="2026-03-28T00:00:00Z",
            trajectory=trajectory,
            actual_window=actual,
            event_detected=False,
            config=build_config(horizon_minutes=2),
        )

        self.assertAlmostEqual(metrics.mae_temp_c, 1.0, places=6)
        self.assertAlmostEqual(metrics.rmse_temp_c, 1.0, places=6)
        self.assertAlmostEqual(metrics.mae_rh_pct, 1.0, places=6)
        self.assertAlmostEqual(metrics.rmse_rh_pct, 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
