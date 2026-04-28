# File overview:
# - Responsibility: Provides regression coverage for schedule rate behavior.
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

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.schedule import ActiveHoursSchedule
# Class purpose: Groups related regression checks for ScheduleRate behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class ScheduleRateTests(unittest.TestCase):
    # Test purpose: Verifies that active hours use higher event rate behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ScheduleRateTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_active_hours_use_higher_event_rate(self) -> None:
        schedule = ActiveHoursSchedule(active_start_hour=8, active_end_hour=18)
        inactive_uptime = 2 * 3600.0
        active_uptime = 9 * 3600.0

        self.assertFalse(schedule.is_active(inactive_uptime))
        self.assertTrue(schedule.is_active(active_uptime))
        self.assertEqual(
            schedule.event_rate_per_hour(base_rate_per_hour=0.2, active_rate_per_hour=1.1, uptime_s=inactive_uptime),
            0.2,
        )
        self.assertEqual(
            schedule.event_rate_per_hour(base_rate_per_hour=0.2, active_rate_per_hour=1.1, uptime_s=active_uptime),
            1.1,
        )
        self.assertLess(schedule.noise_multiplier(inactive_uptime), schedule.noise_multiplier(active_uptime))
    # Test purpose: Verifies that start hour offset aligns schedule to local
    #   clock behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ScheduleRateTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_start_hour_offset_aligns_schedule_to_local_clock(self) -> None:
        schedule = ActiveHoursSchedule(active_start_hour=8, active_end_hour=18)

        self.assertTrue(schedule.is_active(0.0, start_hour_offset=9.0))
        self.assertFalse(schedule.is_active(0.0, start_hour_offset=3.0))
        self.assertEqual(
            schedule.event_rate_per_hour(
                base_rate_per_hour=0.2,
                active_rate_per_hour=0.8,
                uptime_s=0.0,
                start_hour_offset=9.0,
            ),
            0.8,
        )


if __name__ == "__main__":
    unittest.main()
