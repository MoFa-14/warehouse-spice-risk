# File overview:
# - Responsibility: Provides regression coverage for event detection behavior.
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
# Class purpose: Groups related regression checks for EventDetection behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class EventDetectionTests(unittest.TestCase):
    # Test purpose: Verifies that recent RH spike is detected as door open like
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on EventDetectionTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_recent_rh_spike_is_detected_as_door_open_like(self) -> None:
        window = synthetic_window()
        for index in range(174, 180):
            window[index] = window[index].__class__(
                ts_utc=window[index].ts_utc,
                temp_c=window[index].temp_c + 0.05 * (index - 173),
                rh_pct=window[index].rh_pct + 2.0 * (index - 173),
                dew_point_c=window[index].dew_point_c + 0.4 * (index - 173),
                observed=True,
            )

        detection = detect_recent_event(window, config=ForecastConfig())

        self.assertTrue(detection.event_detected)
        self.assertEqual(detection.event_type, "door_open_like")
        self.assertIn("dRH5", detection.event_reason)
    # Test purpose: Verifies that stable window does not trigger event behaves
    #   as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on EventDetectionTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_stable_window_does_not_trigger_event(self) -> None:
        detection = detect_recent_event(synthetic_window(), config=ForecastConfig())

        self.assertFalse(detection.event_detected)
        self.assertEqual(detection.event_type, "none")


if __name__ == "__main__":
    unittest.main()
