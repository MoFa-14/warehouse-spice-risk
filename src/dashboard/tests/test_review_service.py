# File overview:
# - Responsibility: Provides regression coverage for review service behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import json
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

from app.services.review_service import build_monitoring_review_context
from app.services.timeseries_service import resolve_time_window
# Class purpose: Groups related regression checks for MonitoringReviewService
#   behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class MonitoringReviewServiceTests(unittest.TestCase):
    # Test purpose: Verifies that review summary reports excursions trends and
    #   recommendation events behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MonitoringReviewServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_review_summary_reports_excursions_trends_and_recommendation_events(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            db_path = data_root / "db" / "telemetry.sqlite"
            acks_file = Path(temp_dir) / "runtime" / "acks.json"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            acks_file.parent.mkdir(parents=True, exist_ok=True)
            acks_file.write_text(json.dumps({"01|2|Humidity above ideal: clumping risk increasing": "2026-03-29T14:30:00+00:00"}), encoding="utf-8")

            start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
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
                    CREATE TABLE gateway_events (
                        ts_pc_utc TEXT NOT NULL,
                        level TEXT NOT NULL,
                        pod_id TEXT,
                        message TEXT NOT NULL
                    )
                    """
                )

                samples = [
                    (start.isoformat().replace("+00:00", "Z"), "01", 0, 1, 10.0, 19.0, 45.0, 0, -45, "", "BLE"),
                    ((start + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"), "01", 0, 2, 40.0, 23.4, 55.0, 0, -44, "", "BLE"),
                    ((start + timedelta(minutes=60)).isoformat().replace("+00:00", "Z"), "01", 0, 3, 70.0, 25.6, 66.0, 0, -43, "", "BLE"),
                ]
                connection.executemany(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    samples,
                )
                link_rows = [
                    (start.isoformat().replace("+00:00", "Z"), "01", 1, -45, 1, 0, 0, 0, 0, 0.0),
                    ((start + timedelta(minutes=60)).isoformat().replace("+00:00", "Z"), "01", 1, -43, 3, 2, 1, 0, 1, 0.4),
                ]
                connection.executemany(
                    """
                    INSERT INTO link_quality (
                        ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                        total_duplicates, disconnect_count, reconnect_count, missing_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    link_rows,
                )
                connection.execute(
                    """
                    INSERT INTO forecasts (
                        ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                        event_detected, event_type, event_reason, model_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (start + timedelta(minutes=60)).isoformat().replace("+00:00", "Z"),
                        "01",
                        "baseline",
                        30,
                        json.dumps({"temp_forecast_c": [25.6] * 30, "rh_forecast_pct": [66.0] * 30, "dew_point_forecast_c": [18.5] * 30}),
                        json.dumps({"temp_c": [25.0] * 30, "rh_pct": [65.0] * 30}),
                        json.dumps({"temp_c": [26.0] * 30, "rh_pct": [67.0] * 30}),
                        1,
                        "door_open_like",
                        "recent rise",
                        "forecasting-v1",
                    ),
                )
                connection.execute(
                    "INSERT INTO gateway_events (ts_pc_utc, level, pod_id, message) VALUES (?, ?, ?, ?)",
                    ((start + timedelta(minutes=65)).isoformat().replace("+00:00", "Z"), "warning", "01", "resend_request from_seq=2"),
                )
                connection.commit()

            display_timezone = timezone.utc
            window = resolve_time_window("24h", None, None, display_timezone=display_timezone, reference_end=start + timedelta(hours=2))
            context = build_monitoring_review_context(
                data_root,
                window=window,
                db_path=db_path,
                pod_id="01",
                acks_file=acks_file,
                now=start + timedelta(hours=2),
            )

            self.assertEqual(context["summary"]["pod_count"], 1)
            self.assertEqual(context["summary"]["excursion_count"], 1)
            self.assertEqual(context["summary"]["recommendation_event_count"], 1)
            self.assertEqual(context["summary"]["active_acknowledgement_count"], 1)
            row = context["rows"][0]
            self.assertEqual(row.worst_level_label, "Critical")
            self.assertIn("Rising", row.temp_trend_summary)
            self.assertIn("Rising", row.rh_trend_summary)
            self.assertEqual(row.link_missing_samples, 2)
            self.assertEqual(row.duplicate_count, 1)
            self.assertEqual(row.reconnect_count, 1)
            self.assertEqual(row.gateway_warning_count, 1)


if __name__ == "__main__":
    unittest.main()
