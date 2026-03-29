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


class DashboardTimezoneTests(unittest.TestCase):
    def test_display_format_uses_local_dashboard_timezone(self) -> None:
        bst = timezone(timedelta(hours=1), "BST")
        source = datetime(2026, 3, 29, 13, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(format_display_timestamp(source, bst), "2026-03-29 14:00:00 BST")

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
