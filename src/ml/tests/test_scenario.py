# File overview:
# - Responsibility: Provides regression coverage for scenario behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import ForecastConfig
from forecasting.scenario import build_event_persist_forecast
# Class purpose: Groups related regression checks for EventPersistScenario behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class EventPersistScenarioTests(unittest.TestCase):
    # Test purpose: Verifies that event persist RH uses decay and total drift
    #   cap behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on EventPersistScenarioTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_event_persist_rh_uses_decay_and_total_drift_cap(self) -> None:
        window = synthetic_window(
            temp_base=24.0,
            rh_base=39.04,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        rh_tail = [39.04, 37.10, 36.64, 35.74, 35.29, 35.52]
        for offset, rh_value in enumerate(rh_tail, start=len(window) - len(rh_tail)):
            point = window[offset]
            window[offset] = point.__class__(
                ts_utc=point.ts_utc,
                temp_c=point.temp_c,
                rh_pct=rh_value,
                dew_point_c=point.dew_point_c,
                observed=True,
            )

        forecast = build_event_persist_forecast(window, config=ForecastConfig())

        self.assertEqual(forecast.source, "event_persist_slope")
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 34.816, places=3)
        self.assertGreater(forecast.rh_forecast_pct[9], 31.0)
        self.assertGreaterEqual(min(forecast.rh_forecast_pct), 31.52 - 1e-6)
        first_drop = forecast.rh_forecast_pct[0] - forecast.rh_forecast_pct[1]
        later_drop = forecast.rh_forecast_pct[5] - forecast.rh_forecast_pct[6]
        self.assertGreater(first_drop, later_drop)
        self.assertIn("exponential RH decay", forecast.notes)


if __name__ == "__main__":
    unittest.main()
