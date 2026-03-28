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
from app.services.pod_service import get_latest_pod_reading


class DashboardDataPathTests(unittest.TestCase):
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
            connection.commit()


if __name__ == "__main__":
    unittest.main()
