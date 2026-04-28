# File overview:
# - Responsibility: Provides regression coverage for dew point behavior.
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

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.preprocess.dewpoint import dew_point_c
# Class purpose: Groups related regression checks for dew point behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class DewPointTests(unittest.TestCase):
    # Test purpose: Verifies that magnus formula matches expected value behaves
    #   as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DewPointTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_magnus_formula_matches_expected_value(self) -> None:
        self.assertAlmostEqual(dew_point_c(20.0, 50.0) or 0.0, 9.26, places=1)
    # Test purpose: Verifies that returns none for missing values behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DewPointTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_returns_none_for_missing_values(self) -> None:
        self.assertIsNone(dew_point_c(None, 50.0))
        self.assertIsNone(dew_point_c(20.0, None))
    # Test purpose: Verifies that clamps humidity into supported range behaves
    #   as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DewPointTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_clamps_humidity_into_supported_range(self) -> None:
        self.assertAlmostEqual(dew_point_c(20.0, 120.0) or 0.0, 20.0, places=2)
        self.assertAlmostEqual(
            dew_point_c(20.0, 0.0) or 0.0,
            dew_point_c(20.0, -10.0) or 0.0,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
