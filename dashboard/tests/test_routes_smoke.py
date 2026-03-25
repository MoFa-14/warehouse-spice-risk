from __future__ import annotations

import csv
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.main import create_app


class DashboardRoutesSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        base = Path(self.temp_dir.name)
        self.data_root = base / "data"
        (self.data_root / "raw" / "pods" / "01").mkdir(parents=True, exist_ok=True)
        (self.data_root / "raw" / "link_quality").mkdir(parents=True, exist_ok=True)
        runtime_dir = base / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        day = now.date().isoformat()

        with (self.data_root / "raw" / "pods" / "01" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "flags", "rssi", "quality_flags"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "01", 1, 5.0, 25.2, 65.0, 0, -43, 0])

        with (self.data_root / "raw" / "link_quality" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "connected", "last_rssi", "total_received", "total_missing", "total_duplicates", "disconnect_count", "reconnect_count", "missing_rate"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "01", 1, -43, 1, 0, 0, 0, 0, 0.0])

        self.app = create_app(
            {
                "TESTING": True,
                "DATA_ROOT": self.data_root,
                "ACKS_FILE": runtime_dir / "acks.json",
                "RUNTIME_DIR": runtime_dir,
                "SECRET_KEY": "test-key",
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_core_routes_return_200(self) -> None:
        for route in ("/", "/pods/01", "/health", "/alerts", "/prediction"):
            response = self.client.get(route)
            self.assertEqual(response.status_code, 200, route)

    def test_pages_render_expected_text(self) -> None:
        self.assertIn(b"Pod 01", self.client.get("/").data)
        self.assertIn(b"Temperature vs Time", self.client.get("/pods/01").data)
        self.assertIn(b"CRITICAL", self.client.get("/alerts").data)
        self.assertIn(b"Not implemented yet", self.client.get("/prediction").data)

    def test_acknowledge_post_redirects_back_to_alerts(self) -> None:
        alerts_page = self.client.get("/alerts")
        self.assertIn(b"Ack 30m", alerts_page.data)
        response = self.client.post(
            "/alerts/acknowledge",
            data={
                "ack_key": "01|4|Rapid mold growth risk; Severe heat: rapid aroma/color degradation",
                "next": "/alerts",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
