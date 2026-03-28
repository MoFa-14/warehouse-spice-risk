from __future__ import annotations

import sys
import unittest
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.schedule import ActiveHoursSchedule


class ScheduleRateTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
