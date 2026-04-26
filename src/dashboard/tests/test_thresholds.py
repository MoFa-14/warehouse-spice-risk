from __future__ import annotations

import sys
import unittest
from pathlib import Path


DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.thresholds import threshold_legend


class ThresholdLegendTests(unittest.TestCase):
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
