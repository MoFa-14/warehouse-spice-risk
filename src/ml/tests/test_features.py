# File overview:
# - Responsibility: Provides regression coverage for features behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import unittest

from _helpers import load_fixture_points, synthetic_window
from forecasting.features import extract_feature_vector
# Class purpose: Groups related regression checks for FeatureExtraction behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class FeatureExtractionTests(unittest.TestCase):
    # Test purpose: Verifies that extract features includes time and recent
    #   trends behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on FeatureExtractionTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_extract_features_includes_time_and_recent_trends(self) -> None:
        window = synthetic_window(temp_rate_per_min=0.05, rh_rate_per_min=-0.03)

        feature_vector = extract_feature_vector(window)

        self.assertIn("hour_sin", feature_vector.values)
        self.assertIn("hour_cos", feature_vector.values)
        self.assertGreater(feature_vector.values["temp_slope_30"], 0.0)
        self.assertLess(feature_vector.values["rh_slope_30"], 0.0)
        self.assertEqual(feature_vector.observed_points, 180)
    # Test purpose: Verifies that fixture file loads for small realistic window
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on FeatureExtractionTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_fixture_file_loads_for_small_realistic_window(self) -> None:
        points = load_fixture_points()

        feature_vector = extract_feature_vector(points)

        self.assertEqual(len(points), 37)
        self.assertEqual(feature_vector.observed_points, 37)
        self.assertAlmostEqual(feature_vector.values["temp_last"], points[-1].temp_c, places=6)


if __name__ == "__main__":
    unittest.main()
