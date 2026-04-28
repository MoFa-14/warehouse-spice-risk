# File overview:
# - Responsibility: Provides regression coverage for alerts behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from pathlib import Path

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.thresholds import classify_storage_conditions, classify_storage_trajectory
# Class purpose: Groups related regression checks for AlertsThreshold behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class AlertsThresholdTests(unittest.TestCase):
    # Test purpose: Verifies that humidity boundaries behaves as expected under
    #   this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_humidity_boundaries(self) -> None:
        self.assertEqual(classify_storage_conditions(20.0, 65.0).level, 4)
        self.assertEqual(classify_storage_conditions(20.0, 60.1).level, 3)
        self.assertEqual(classify_storage_conditions(20.0, 50.1).level, 2)
        self.assertEqual(classify_storage_conditions(20.0, 29.0).level, 2)
    # Test purpose: Verifies that temperature boundaries behaves as expected
    #   under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_temperature_boundaries(self) -> None:
        self.assertEqual(classify_storage_conditions(22.1, 40.0).level, 2)
        self.assertEqual(classify_storage_conditions(25.0, 40.0).level, 4)
    # Test purpose: Verifies that combined conditions choose max severity
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_combined_conditions_choose_max_severity(self) -> None:
        self.assertEqual(classify_storage_conditions(23.0, 61.0).level, 3)
        self.assertEqual(classify_storage_conditions(25.0, 66.0).level, 4)
    # Test purpose: Verifies that optimal and acceptable levels behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_optimal_and_acceptable_levels(self) -> None:
        self.assertEqual(classify_storage_conditions(15.0, 40.0).level, 0)
        self.assertEqual(classify_storage_conditions(9.5, 40.0).level, 1)
    # Test purpose: Verifies that trajectory uses worst predicted level behaves
    #   as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_trajectory_uses_worst_predicted_level(self) -> None:
        trajectory = classify_storage_trajectory(
            [20.0, 20.5, 21.0, 21.5],
            [49.0, 52.0, 61.0, 66.0],
        )
        self.assertIsNotNone(trajectory)
        self.assertEqual(trajectory.status.level, 4)
        self.assertEqual(trajectory.horizon_minute, 4)
    # Test purpose: Verifies that trajectory keeps first occurrence of worst
    #   level behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on AlertsThresholdTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_trajectory_keeps_first_occurrence_of_worst_level(self) -> None:
        trajectory = classify_storage_trajectory(
            [25.0, 25.4, 25.8],
            [40.0, 42.0, 44.0],
        )
        self.assertIsNotNone(trajectory)
        self.assertEqual(trajectory.status.level, 4)
        self.assertEqual(trajectory.horizon_minute, 1)


if __name__ == "__main__":
    unittest.main()
