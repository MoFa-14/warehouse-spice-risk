# File overview:
# - Responsibility: Provides regression coverage for filtering behavior.
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
from forecasting.event_detection import detect_recent_event
from forecasting.filtering import build_baseline_window
# Class purpose: Groups related regression checks for Filtering behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class FilteringTests(unittest.TestCase):
    # Test purpose: Verifies that baseline filter reduces spike influence
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on FilteringTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_baseline_filter_reduces_spike_influence(self) -> None:
        window = synthetic_window(temp_rate_per_min=0.0, rh_rate_per_min=0.0)
        for index in range(175, 180):
            window[index] = window[index].__class__(
                ts_utc=window[index].ts_utc,
                temp_c=window[index].temp_c + 0.8 * (index - 174),
                rh_pct=window[index].rh_pct + 0.3 * (index - 174),
                dew_point_c=window[index].dew_point_c,
                observed=True,
            )

        config = ForecastConfig()
        detection = detect_recent_event(window, config=config)
        filtered = build_baseline_window(window, detection=detection, config=config)

        self.assertTrue(detection.event_detected)
        self.assertLess(filtered[-1].temp_c, window[-1].temp_c)
        self.assertLess(abs(filtered[-1].temp_c - filtered[-6].temp_c), abs(window[-1].temp_c - window[-6].temp_c))


if __name__ == "__main__":
    unittest.main()
