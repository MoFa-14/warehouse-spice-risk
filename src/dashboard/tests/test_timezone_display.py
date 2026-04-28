# File overview:
# - Responsibility: Provides regression coverage for timezone display behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.timezone import format_display_timestamp
from app.services.timeseries_service import resolve_time_window
# Class purpose: Groups related regression checks for DashboardTimezone behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class DashboardTimezoneTests(unittest.TestCase):
    # Test purpose: Verifies that display format uses local dashboard timezone
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardTimezoneTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_display_format_uses_local_dashboard_timezone(self) -> None:
        bst = timezone(timedelta(hours=1), "BST")
        source = datetime(2026, 3, 29, 13, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(format_display_timestamp(source, bst), "2026-03-29 14:00:00 BST")
    # Test purpose: Verifies that custom range input is interpreted in display
    #   timezone behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardTimezoneTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_custom_range_input_is_interpreted_in_display_timezone(self) -> None:
        bst = timezone(timedelta(hours=1), "BST")

        window = resolve_time_window(
            "custom",
            "2026-03-29T14:00",
            "2026-03-29T15:00",
            display_timezone=bst,
        )

        self.assertEqual(window.start.isoformat().replace("+00:00", "Z"), "2026-03-29T13:00:00Z")
        self.assertEqual(window.end.isoformat().replace("+00:00", "Z"), "2026-03-29T14:00:00Z")


if __name__ == "__main__":
    unittest.main()
