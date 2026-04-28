# File overview:
# - Responsibility: Provides regression coverage for data path behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import csv
import sqlite3
import sys
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.data_access.csv_reader import read_link_quality, read_raw_samples
from app.services.link_service import build_health_context
from app.services.pod_service import discover_dashboard_pods, get_latest_pod_reading
# Class purpose: Groups related regression checks for DashboardDataPath behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class DashboardDataPathTests(unittest.TestCase):
    # Test purpose: Verifies that raw reader preserves leading zero pod ids and
    #   append order behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_raw_reader_preserves_leading_zero_pod_ids_and_append_order(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "raw.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T10:00:00Z", "01", 1, 5.0, 20.0, 50.0, 9.26, 0, -60, 0])
                writer.writerow(["2026-03-25T10:00:00Z", "01", 2, 10.0, 21.0, 51.0, 10.30, 0, -59, 0])

            frame = read_raw_samples([csv_path])

            self.assertEqual(frame.iloc[0]["pod_id"], "01")
            self.assertEqual(frame.iloc[-1]["seq"], 2)
    # Test purpose: Verifies that latest reading uses newest sample even when it
    #   is incomplete behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_latest_reading_uses_newest_sample_even_when_it_is_incomplete(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            pod_dir = data_root / "raw" / "pods" / "01"
            pod_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-25.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T19:26:15Z", "01", 30, 3600.0, 24.86, 32.09, 7.070487, 0, -40, 0])
                writer.writerow(["2026-03-25T19:59:15Z", "01", 36, 3708.6, "", "", "", 1, -37, 21])

            reading = get_latest_pod_reading(data_root, "01")

            self.assertIsNotNone(reading)
            self.assertEqual(reading.pod_id, "01")
            self.assertEqual(reading.ts_pc_utc.isoformat().replace("+00:00", "Z"), "2026-03-25T19:59:15Z")
            self.assertFalse(reading.has_measurement)
            self.assertIsNone(reading.temp_c)
            self.assertIsNone(reading.rh_pct)
            self.assertEqual(reading.last_complete_ts_pc_utc.isoformat().replace("+00:00", "Z"), "2026-03-25T19:26:15Z")
    # Test purpose: Verifies that health context matches link rows for zero
    #   padded pod ids behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_health_context_matches_link_rows_for_zero_padded_pod_ids(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            pod_dir = data_root / "raw" / "pods" / "01"
            link_dir = data_root / "raw" / "link_quality"
            pod_dir.mkdir(parents=True, exist_ok=True)
            link_dir.mkdir(parents=True, exist_ok=True)

            with (pod_dir / "2026-03-25.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T19:26:15Z", "01", 30, 3600.0, 24.86, 32.09, 7.070487, 0, -40, 0])

            link_path = link_dir / "2026-03-25.csv"
            with link_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "connected", "last_rssi", "total_received", "total_missing", "total_duplicates", "disconnect_count", "reconnect_count", "missing_rate"])
                writer.writerow(["2026-03-25T19:30:00Z", "01", 1, -40, 12, 1, 0, 0, 0, 0.08])

            link_frame = read_link_quality([link_path])
            self.assertEqual(link_frame.iloc[0]["pod_id"], "01")

            health = build_health_context(data_root)

            self.assertEqual(len(health["rows"]), 1)
            self.assertEqual(health["rows"][0].pod_id, "01")
            self.assertEqual(health["rows"][0].total_received, 12.0)
    # Test purpose: Verifies that latest reading prefers SQLite live data over
    #   stale CSV behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_latest_reading_prefers_sqlite_live_data_over_stale_csv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            pod_dir = data_root / "raw" / "pods" / "01"
            db_dir = data_root / "db"
            pod_dir.mkdir(parents=True, exist_ok=True)
            db_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-25.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T19:26:15Z", "01", 30, 3600.0, 18.0, 40.0, 4.173479, 0, -40, 0])

            db_path = db_dir / "telemetry.sqlite"
            self._create_dashboard_sqlite(db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T20:00:00Z", "01", 0, 31, 3610.0, 24.86, 32.09, 0, -37, "", "BLE"),
                )
                connection.commit()

            reading = get_latest_pod_reading(data_root, "01", db_path=db_path)

            self.assertIsNotNone(reading)
            self.assertEqual(reading.ts_pc_utc.isoformat().replace("+00:00", "Z"), "2026-03-25T20:00:00Z")
            self.assertAlmostEqual(reading.temp_c, 24.86)
    # Test purpose: Verifies that health context prefers SQLite link data
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_health_context_prefers_sqlite_link_data(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            pod_dir = data_root / "raw" / "pods" / "01"
            link_dir = data_root / "raw" / "link_quality"
            db_dir = data_root / "db"
            pod_dir.mkdir(parents=True, exist_ok=True)
            link_dir.mkdir(parents=True, exist_ok=True)
            db_dir.mkdir(parents=True, exist_ok=True)

            with (pod_dir / "2026-03-25.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T19:26:15Z", "01", 30, 3600.0, 19.0, 40.0, 5.004163, 0, -40, 0])

            db_path = db_dir / "telemetry.sqlite"
            self._create_dashboard_sqlite(db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T19:30:00Z", "01", 0, 31, 3610.0, 24.0, 41.0, 0, -39, "", "BLE"),
                )
                connection.execute(
                    """
                    INSERT INTO link_quality (
                        ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                        total_duplicates, disconnect_count, reconnect_count, missing_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T19:31:00Z", "01", 1, -39, 31, 2, 1, 0, 1, 0.0606),
                )
                connection.commit()

            health = build_health_context(data_root, db_path=db_path)

            self.assertEqual(len(health["rows"]), 1)
            self.assertEqual(health["rows"][0].pod_id, "01")
            self.assertEqual(health["rows"][0].total_received, 31.0)
            self.assertEqual(health["rows"][0].last_rssi, -39.0)
    # Test purpose: Verifies that dashboard pod discovery keeps stored SQLite
    #   only pods visible behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_dashboard_pod_discovery_keeps_stored_sqlite_only_pods_visible(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            db_dir = data_root / "db"
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / "telemetry.sqlite"
            self._create_dashboard_sqlite(db_path)

            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T19:30:00Z", "01", 0, 1, 60.0, 20.0, 45.0, 0, -40, "", "BLE"),
                )
                connection.execute(
                    """
                    INSERT INTO link_quality (
                        ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                        total_duplicates, disconnect_count, reconnect_count, missing_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T19:31:00Z", "08", 0, -55, 15, 1, 0, 1, 1, 0.0625),
                )
                connection.execute(
                    """
                    INSERT INTO forecasts (
                        ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                        event_detected, event_type, event_reason, model_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "2026-03-25T19:32:00Z",
                        "10",
                        "baseline",
                        30,
                        '{"temp_forecast_c":[20.0],"rh_forecast_pct":[45.0],"dew_point_forecast_c":[7.5]}',
                        '{"temp_c":[19.8],"rh_pct":[44.8]}',
                        '{"temp_c":[20.2],"rh_pct":[45.2]}',
                        0,
                        "none",
                        "",
                        "forecasting-v1",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO evaluations (
                        ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH, event_detected, large_error, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T19:32:00Z", "12", "baseline", 0.2, 0.3, 1.0, 1.5, 0, 0, "ok"),
                )
                connection.commit()

            self.assertEqual(discover_dashboard_pods(data_root, db_path=db_path), ["01", "08", "10", "12"])
    # Method purpose: Creates dashboard SQLite in the form expected by later
    #   code.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardDataPathTests.
    # - Inputs: Arguments such as db_path, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    @staticmethod
    def _create_dashboard_sqlite(db_path: Path) -> None:
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
            connection.commit()


if __name__ == "__main__":
    unittest.main()
