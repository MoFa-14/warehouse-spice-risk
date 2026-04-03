from __future__ import annotations

import csv
import json
import math
import sqlite3
import sys
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
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
        (self.data_root / "raw" / "pods" / "02").mkdir(parents=True, exist_ok=True)
        (self.data_root / "raw" / "link_quality").mkdir(parents=True, exist_ok=True)
        (self.data_root / "db").mkdir(parents=True, exist_ok=True)
        runtime_dir = base / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        now = datetime(2026, 3, 29, 13, 0, 0, tzinfo=timezone.utc)
        day = now.date().isoformat()
        dew_point = self._dew_point_c(25.2, 65.0)
        db_path = self.data_root / "db" / "telemetry.sqlite"

        with (self.data_root / "raw" / "pods" / "01" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "01", 1, 5.0, 19.0, 40.0, f"{self._dew_point_c(19.0, 40.0):.6f}", 0, -43, 0])
        with (self.data_root / "raw" / "pods" / "02" / f"{day}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
            writer.writerow([now.isoformat().replace("+00:00", "Z"), "02", 1, 5.0, 18.1, 51.4, f"{self._dew_point_c(18.1, 51.4):.6f}", 0, -51, 0])

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
                CREATE TABLE forecasts (
                    ts_pc_utc TEXT NOT NULL,
                    pod_id TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    json_forecast TEXT NOT NULL,
                    json_p25 TEXT NOT NULL,
                    json_p75 TEXT NOT NULL,
                    event_detected INTEGER NOT NULL,
                    event_type TEXT,
                    event_reason TEXT,
                    model_version TEXT NOT NULL,
                    PRIMARY KEY (pod_id, ts_pc_utc, scenario)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE evaluations (
                    ts_forecast_utc TEXT NOT NULL,
                    pod_id TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    MAE_T REAL NOT NULL,
                    RMSE_T REAL NOT NULL,
                    MAE_RH REAL NOT NULL,
                    RMSE_RH REAL NOT NULL,
                    event_detected INTEGER NOT NULL,
                    large_error INTEGER NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (pod_id, ts_forecast_utc, scenario)
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
                INSERT INTO samples_raw (
                    ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "02", 0, 8, 50.0, 18.1, 51.4, 0, -51, "", "TCP"),
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
            connection.execute(
                """
                INSERT INTO link_quality (
                    ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                    total_duplicates, disconnect_count, reconnect_count, missing_rate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "02", 1, -51, 8, 0, 0, 0, 0, 0.0),
            )
            baseline_json = json.dumps(
                {
                    "temp_forecast_c": [25.2 + 0.02 * index for index in range(30)],
                    "rh_forecast_pct": [65.0 - 0.15 * index for index in range(30)],
                    "dew_point_forecast_c": [dew_point - 0.03 * index for index in range(30)],
                    "feature_vector": {"temp_last": 25.2, "rh_last": 65.0},
                    "missing_rate": 0.0,
                    "source": "analogue_knn",
                    "neighbor_count": 6,
                    "case_count": 6,
                    "notes": "Median forecast over 6 nearest historical cases.",
                }
            )
            event_json = json.dumps(
                {
                    "temp_forecast_c": [25.2 + 0.08 * index for index in range(30)],
                    "rh_forecast_pct": [65.0 + 0.10 * index for index in range(30)],
                    "dew_point_forecast_c": [dew_point + 0.02 * index for index in range(30)],
                    "feature_vector": {"temp_last": 25.2, "rh_last": 65.0},
                    "missing_rate": 0.0,
                    "source": "event_persist_slope",
                    "neighbor_count": 0,
                    "case_count": 0,
                    "notes": "Continues the current 5-minute raw slope with deterministic rate caps.",
                }
            )
            p25_json = json.dumps({"temp_c": [24.9 + 0.02 * index for index in range(30)], "rh_pct": [64.5 - 0.15 * index for index in range(30)]})
            p75_json = json.dumps({"temp_c": [25.5 + 0.02 * index for index in range(30)], "rh_pct": [65.5 - 0.15 * index for index in range(30)]})
            event_p25_json = json.dumps({"temp_c": [24.8 + 0.08 * index for index in range(30)], "rh_pct": [64.7 + 0.10 * index for index in range(30)]})
            event_p75_json = json.dumps({"temp_c": [25.6 + 0.08 * index for index in range(30)], "rh_pct": [65.3 + 0.10 * index for index in range(30)]})
            connection.execute(
                """
                INSERT INTO forecasts (
                    ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                    event_detected, event_type, event_reason, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "01", "baseline", 30, baseline_json, p25_json, p75_json, 1, "door_open_like", "dRH5=8.2% thrRH=4.5% dew15=1.1C run=2", "forecasting-v1"),
            )
            connection.execute(
                """
                INSERT INTO forecasts (
                    ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                    event_detected, event_type, event_reason, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "01", "event_persist", 30, event_json, event_p25_json, event_p75_json, 1, "door_open_like", "dRH5=8.2% thrRH=4.5% dew15=1.1C run=2", "forecasting-v1"),
            )
            connection.execute(
                """
                INSERT INTO evaluations (
                    ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH, event_detected, large_error, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "01", "baseline", 0.21, 0.29, 1.10, 1.53, 1, 0, "ok"),
            )
            pod02_baseline_json = json.dumps(
                {
                    "temp_forecast_c": [18.1 + 0.01 * index for index in range(30)],
                    "rh_forecast_pct": [51.4 + 0.03 * index for index in range(30)],
                    "dew_point_forecast_c": [self._dew_point_c(18.1, 51.4) + 0.01 * index for index in range(30)],
                    "feature_vector": {"temp_last": 18.1, "rh_last": 51.4},
                    "missing_rate": 0.0,
                    "source": "fallback_persistence",
                    "neighbor_count": 0,
                    "case_count": 0,
                    "notes": "Case base smaller than minimum analogue threshold; used bounded slope persistence.",
                }
            )
            pod02_p25_json = json.dumps({"temp_c": [18.0 + 0.01 * index for index in range(30)], "rh_pct": [51.0 + 0.03 * index for index in range(30)]})
            pod02_p75_json = json.dumps({"temp_c": [18.2 + 0.01 * index for index in range(30)], "rh_pct": [51.8 + 0.03 * index for index in range(30)]})
            connection.execute(
                """
                INSERT INTO forecasts (
                    ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                    event_detected, event_type, event_reason, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now.isoformat().replace("+00:00", "Z"), "02", "baseline", 30, pod02_baseline_json, pod02_p25_json, pod02_p75_json, 0, "none", "no_recent_event temp_thr_5m=1.20C rh_thr_5m=3.00%", "forecasting-v1"),
            )
            connection.commit()

        self.app = create_app(
            {
                "TESTING": True,
                "DATA_ROOT": self.data_root,
                "DB_PATH": db_path,
                "ACKS_FILE": runtime_dir / "acks.json",
                "RUNTIME_DIR": runtime_dir,
                "DISPLAY_TIMEZONE": timezone(timedelta(hours=1), "BST"),
                "SECRET_KEY": "test-key",
            }
        )
        self.client = self.app.test_client()
        self.expected_dew_text = f"{dew_point:.2f}".encode()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_core_routes_return_200(self) -> None:
        for route in ("/", "/pods/01", "/health", "/alerts", "/prediction", "/review"):
            response = self.client.get(route)
            self.assertEqual(response.status_code, 200, route)

    def test_pages_render_expected_text(self) -> None:
        overview = self.client.get("/").data
        detail = self.client.get("/pods/01").data
        self.assertIn(b"Pod 01", overview)
        self.assertIn(self.expected_dew_text, overview)
        self.assertIn(b"2026-03-29 14:00:00 BST", overview)
        self.assertIn(b"Temperature vs Time", detail)
        self.assertIn(b"Dew Point vs Time", detail)
        self.assertIn(self.expected_dew_text, detail)
        self.assertIn(b"Start (BST)", detail)
        self.assertIn(b"CRITICAL", self.client.get("/alerts").data)
        prediction = self.client.get("/prediction").data
        self.assertIn(b"Latest warehouse forecasts", prediction)
        self.assertIn(b"Pod 01 30-minute outlook", prediction)
        self.assertIn(b"Pod 02 30-minute outlook", prediction)
        self.assertIn(b"Jump to forecast", prediction)
        self.assertIn(b"Baseline", prediction)
        self.assertIn(b"CRITICAL", prediction)
        self.assertIn(b"WARNING", prediction)
        self.assertIn(b"Event persist", prediction)
        self.assertIn(b"door_open_like", prediction)
        self.assertIn(b"Immediate action required", prediction)
        self.assertIn(b"Monitor closely; improve ventilation/dehumidification if trend continues.", prediction)
        self.assertIn(b"30-minute outlook", detail)
        review = self.client.get("/review").data
        self.assertIn(b"review summary", review)
        self.assertIn(b"Threshold excursions", review)
        self.assertIn(b"Recommendation-triggering events", review)

    def test_dashboard_disables_auto_refresh_by_default(self) -> None:
        response = self.client.get("/")
        self.assertNotIn(b'http-equiv="refresh"', response.data)

    def test_dashboard_can_opt_in_to_auto_refresh(self) -> None:
        app = create_app(
            {
                "TESTING": True,
                "DATA_ROOT": self.data_root,
                "DB_PATH": self.data_root / "db" / "telemetry.sqlite",
                "ACKS_FILE": Path(self.temp_dir.name) / "runtime" / "acks.json",
                "RUNTIME_DIR": Path(self.temp_dir.name) / "runtime",
                "DISPLAY_TIMEZONE": timezone(timedelta(hours=1), "BST"),
                "AUTO_REFRESH_SECONDS": 5,
                "SECRET_KEY": "test-key",
            }
        )
        response = app.test_client().get("/")
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

    def test_latest_api_route_returns_json(self) -> None:
        response = self.client.get("/api/pods/01/latest")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["pod_id"], "01")
        self.assertEqual(payload["status"], "CRITICAL")
        self.assertAlmostEqual(payload["temp_c"], 25.2)
        self.assertAlmostEqual(payload["rh_pct"], 65.0)
        self.assertEqual(payload["ts_pc_utc"], "2026-03-29T13:00:00Z")

    @staticmethod
    def _dew_point_c(temp_c: float, rh_pct: float) -> float:
        rh = max(1e-6, min(rh_pct, 100.0)) / 100.0
        a, b = 17.62, 243.12
        gamma = (a * temp_c / (b + temp_c)) + math.log(rh)
        return (b * gamma) / (a - gamma)


if __name__ == "__main__":
    unittest.main()
