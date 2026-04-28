# File overview:
# - Responsibility: Provides regression coverage for thresholds behavior.
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

from app.services.thresholds import threshold_legend
# Class purpose: Groups related regression checks for ThresholdLegend behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class ThresholdLegendTests(unittest.TestCase):
    # Test purpose: Verifies that threshold legend exposes structured cards for
    #   pod detail behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ThresholdLegendTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_threshold_legend_exposes_structured_cards_for_pod_detail(self) -> None:
        legend = threshold_legend()

        self.assertEqual([item["metric"] for item in legend], ["Temperature", "Relative Humidity"])
        self.assertEqual(legend[0]["theme"], "temp")
        self.assertEqual(legend[1]["theme"], "rh")
        self.assertEqual(
            [band["label"] for band in legend[0]["bands"]],
            ["Optimal", "Warning", "Critical"],
        )
        self.assertEqual(
            [band["label"] for band in legend[1]["bands"]],
            ["Ideal", "Warning", "High Risk", "Critical"],
        )
        self.assertIn("Project rule", str(legend[0]["note"]))
        self.assertIn("Below 30%", str(legend[1]["note"]))


if __name__ == "__main__":
    unittest.main()
