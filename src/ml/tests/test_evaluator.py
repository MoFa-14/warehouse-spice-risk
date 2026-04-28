# File overview:
# - Responsibility: Provides regression coverage for evaluator behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from _helpers import ML_SRC  # noqa: F401
from forecasting.config import build_config
from forecasting.evaluator import evaluate_forecast
from forecasting.models import ForecastTrajectory, TimeSeriesPoint
# Class purpose: Groups related regression checks for Evaluator behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class EvaluatorTests(unittest.TestCase):
    # Test purpose: Verifies that MAE and RMSE are computed correctly behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on EvaluatorTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
