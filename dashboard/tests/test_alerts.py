from __future__ import annotations

import sys
import unittest
from pathlib import Path

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.thresholds import classify_storage_conditions


class AlertsThresholdTests(unittest.TestCase):
    def test_humidity_boundaries(self) -> None:
        self.assertEqual(classify_storage_conditions(20.0, 65.0).level, 4)
        self.assertEqual(classify_storage_conditions(20.0, 60.1).level, 3)
        self.assertEqual(classify_storage_conditions(20.0, 50.1).level, 2)
        self.assertEqual(classify_storage_conditions(20.0, 29.0).level, 2)

    def test_temperature_boundaries(self) -> None:
        self.assertEqual(classify_storage_conditions(22.1, 40.0).level, 2)
        self.assertEqual(classify_storage_conditions(25.0, 40.0).level, 4)

    def test_combined_conditions_choose_max_severity(self) -> None:
        self.assertEqual(classify_storage_conditions(23.0, 61.0).level, 3)
        self.assertEqual(classify_storage_conditions(25.0, 66.0).level, 4)

    def test_optimal_and_acceptable_levels(self) -> None:
        self.assertEqual(classify_storage_conditions(15.0, 40.0).level, 0)
        self.assertEqual(classify_storage_conditions(9.5, 40.0).level, 1)


if __name__ == "__main__":
    unittest.main()
