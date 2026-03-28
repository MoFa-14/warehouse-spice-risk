from __future__ import annotations

import csv
import math
import sqlite3
import sys
import unittest
from contextlib import closing
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
        (self.data_root / "db").mkdir(parents=True, exist_ok=True)
        runtime_dir = base / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        day = now.date().isoformat()
        dew_point = self._dew_point_c(25.2, 65.0)
        db_path = self.data_root / "db" / "telemetry.sqlite"

        with (self.data_root / "raw" / "pods" / "01" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "01", 1, 5.0, 19.0, 40.0, f"{self._dew_point_c(19.0, 40.0):.6f}", 0, -43, 0])

        with (self.data_root / "raw" / "link_quality" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "connected", "last_rssi", "total_received", "total_missing", "total_duplicates", "disconnect_count", "reconnect_count", "missing_rate"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "01", 1, -43, 1, 0, 0, 0, 0, 0.0])

        with closing(sqlite3.connect(db_path)) as connection:
            connection.execute(
                """
                CREATE TABLE samples_raw (
                    ts_pc_utc TEXT NOT NULL,
                    pod_id TEXT NOT NULL,
                    session_id INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    ts_uptime_s REAL,
                    temp_c REAL,
                    rh_pct REAL,
                    flags INTEGER,
                    rssi INTEGER,
                    quality_flags TEXT,
                    source TEXT,
                    PRIMARY KEY (pod_id, session_id, seq)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE link_quality (
                    ts_pc_utc TEXT NOT NULL,
                    pod_id TEXT NOT NULL,
                    connected INTEGER,
                    last_rssi INTEGER,
                    total_received INTEGER,
                    total_missing INTEGER,
                    total_duplicates INTEGER,
                    disconnect_count INTEGER,
                    reconnect_count INTEGER,
                    missing_rate REAL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO samples_raw (
                    ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "01", 0, 7, 50.0, 25.2, 65.0, 0, -43, "", "BLE"),
            )
            connection.execute(
                """
                INSERT INTO link_quality (
                    ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                    total_duplicates, disconnect_count, reconnect_count, missing_rate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "01", 1, -43, 7, 0, 0, 0, 0, 0.0),
            )
            connection.commit()

        self.app = create_app(
            {
                "TESTING": True,
                "DATA_ROOT": self.data_root,
                "DB_PATH": db_path,
                "ACKS_FILE": runtime_dir / "acks.json",
                "RUNTIME_DIR": runtime_dir,
                "SECRET_KEY": "test-key",
            }
        )
        self.client = self.app.test_client()
        self.expected_dew_text = f"{dew_point:.2f}".encode()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_core_routes_return_200(self) -> None:
        for route in ("/", "/pods/01", "/health", "/alerts", "/prediction"):
            response = self.client.get(route)
            self.assertEqual(response.status_code, 200, route)

    def test_pages_render_expected_text(self) -> None:
        overview = self.client.get("/").data
        detail = self.client.get("/pods/01").data
        self.assertIn(b"Pod 01", overview)
        self.assertIn(self.expected_dew_text, overview)
        self.assertIn(b"Temperature vs Time", detail)
        self.assertIn(b"Dew Point vs Time", detail)
        self.assertIn(self.expected_dew_text, detail)
        self.assertIn(b"CRITICAL", self.client.get("/alerts").data)
        self.assertIn(b"Not implemented yet", self.client.get("/prediction").data)

    def test_dashboard_includes_auto_refresh_meta_tag(self) -> None:
        response = self.client.get("/")
        self.assertIn(b'http-equiv="refresh"', response.data)
        self.assertIn(b'content="5"', response.data)

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

    @staticmethod
    def _dew_point_c(temp_c: float, rh_pct: float) -> float:
        rh = max(1e-6, min(rh_pct, 100.0)) / 100.0
        a, b = 17.62, 243.12
        gamma = (a * temp_c / (b + temp_c)) + math.log(rh)
        return (b * gamma) / (a - gamma)


if __name__ == "__main__":
    unittest.main()
