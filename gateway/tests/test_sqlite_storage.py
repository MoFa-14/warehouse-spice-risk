from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.link.stats import LinkSnapshot
from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.sqlite_db import connect_sqlite, init_db, initialize_schema
from gateway.storage.sqlite_reader import latest_sample, samples_in_range, utc_bounds_for_dates
from gateway.storage.sqlite_writer import (
    SqliteStorageWriter,
    SqliteWriteResult,
    SqliteWriterDependencies,
    SqliteWriterPipeline,
)


class SqliteStorageTests(unittest.TestCase):
    def test_schema_creation_creates_required_tables(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            init_db(db_path)

            connection = connect_sqlite(db_path, readonly=True)
            try:
                names = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
                    ).fetchall()
                }
                sample_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(samples_raw)").fetchall()
                }
            finally:
                connection.close()

            self.assertIn("samples_raw", names)
            self.assertIn("link_quality", names)
            self.assertIn("gateway_events", names)
            self.assertIn("idx_samples_time", names)
            self.assertIn("idx_samples_pod_time", names)
            self.assertIn("idx_link_time", names)
            self.assertIn("idx_link_pod_time", names)
            self.assertIn("session_id", sample_columns)

    def test_insert_or_ignore_dedupes_same_pod_and_seq(self) -> None:
        with TemporaryDirectory() as temp_dir:
            writer = SqliteStorageWriter(Path(temp_dir) / "telemetry.sqlite")
            record = TelemetryRecord(
                pod_id="01",
                seq=7,
                ts_uptime_s=35.0,
                temp_c=22.5,
                rh_pct=51.2,
                flags=0,
            )

            first = writer.write_sample(
                ts_pc_utc="2026-03-28T12:00:00Z",
                record=record,
                rssi=-63,
                quality_flags=("seq_gap",),
                source="BLE",
            )
            second = writer.write_sample(
                ts_pc_utc="2026-03-28T12:00:00Z",
                record=record,
                rssi=-63,
                quality_flags=("seq_gap",),
                source="BLE",
            )
            writer.close()

            self.assertTrue(first.inserted)
            self.assertFalse(first.duplicate)
            self.assertFalse(second.inserted)
            self.assertTrue(second.duplicate)

            rows = samples_in_range(db_path=Path(temp_dir) / "telemetry.sqlite", pod_id="01")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["seq"], 7)

    def test_range_query_returns_expected_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            writer = SqliteStorageWriter(db_path)
            writer.write_sample(
                ts_pc_utc="2026-03-28T00:00:10Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=10.0,
                    temp_c=20.0,
                    rh_pct=40.0,
                    flags=0,
                ),
                rssi=-60,
                quality_flags=(),
                source="BLE",
            )
            writer.write_sample(
                ts_pc_utc="2026-03-29T00:00:10Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=2,
                    ts_uptime_s=20.0,
                    temp_c=21.0,
                    rh_pct=41.0,
                    flags=0,
                ),
                rssi=-59,
                quality_flags=(),
                source="BLE",
            )
            writer.write_link_snapshot(
                LinkSnapshot(
                    ts_pc_utc="2026-03-28T00:00:30Z",
                    pod_id="01",
                    connected=True,
                    last_rssi=-60,
                    total_received=1,
                    total_missing=0,
                    total_duplicates=0,
                    disconnect_count=0,
                    reconnect_count=0,
                    missing_rate=0.0,
                )
            )
            writer.close()

            ts_from_utc, ts_to_utc = utc_bounds_for_dates(date(2026, 3, 28), date(2026, 3, 28))
            rows = samples_in_range(db_path=db_path, pod_id="01", ts_from_utc=ts_from_utc, ts_to_utc=ts_to_utc)
            latest = latest_sample(db_path=db_path, pod_id="01")

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["seq"], 1)
            self.assertEqual(latest["seq"], 2)

    def test_sequence_restart_reuses_seq_in_new_sqlite_session(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            writer = SqliteStorageWriter(db_path)
            writer.write_sample(
                ts_pc_utc="2026-03-28T00:00:10Z",
                record=TelemetryRecord(
                    pod_id="02",
                    seq=1,
                    ts_uptime_s=10.0,
                    temp_c=24.0,
                    rh_pct=40.0,
                    flags=0,
                ),
                rssi=None,
                quality_flags=(),
                source="TCP",
            )
            writer.write_sample(
                ts_pc_utc="2026-03-28T00:00:20Z",
                record=TelemetryRecord(
                    pod_id="02",
                    seq=2,
                    ts_uptime_s=20.0,
                    temp_c=24.2,
                    rh_pct=40.5,
                    flags=0,
                ),
                rssi=None,
                quality_flags=(),
                source="TCP",
            )
            restarted = writer.write_sample(
                ts_pc_utc="2026-03-28T00:10:00Z",
                record=TelemetryRecord(
                    pod_id="02",
                    seq=1,
                    ts_uptime_s=5.0,
                    temp_c=24.5,
                    rh_pct=41.0,
                    flags=0,
                ),
                rssi=None,
                quality_flags=("sequence_reset",),
                source="TCP",
            )
            writer.close()

            self.assertTrue(restarted.inserted)
            self.assertFalse(restarted.duplicate)

            connection = connect_sqlite(db_path, readonly=True)
            try:
                rows = connection.execute(
                    "SELECT pod_id, session_id, seq FROM samples_raw WHERE pod_id = '02' ORDER BY session_id ASC, seq ASC"
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual([(row["session_id"], row["seq"]) for row in rows], [(0, 1), (0, 2), (1, 1)])

    def test_soft_reload_sequence_drop_with_higher_uptime_opens_new_session(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            writer = SqliteStorageWriter(db_path)
            writer.write_sample(
                ts_pc_utc="2026-03-28T17:26:31Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=104,
                    ts_uptime_s=8207.6,
                    temp_c=18.53,
                    rh_pct=32.25,
                    flags=0,
                ),
                rssi=-43,
                quality_flags=(),
                source="BLE",
            )
            restarted = writer.write_sample(
                ts_pc_utc="2026-03-28T18:30:36Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=89,
                    ts_uptime_s=11640.7,
                    temp_c=18.19,
                    rh_pct=32.31,
                    flags=0,
                ),
                rssi=-43,
                quality_flags=("sequence_reset",),
                source="BLE",
            )
            writer.close()

            self.assertTrue(restarted.inserted)
            self.assertFalse(restarted.duplicate)

            connection = connect_sqlite(db_path, readonly=True)
            try:
                rows = connection.execute(
                    "SELECT session_id, seq, ts_uptime_s FROM samples_raw WHERE pod_id = '01' ORDER BY session_id ASC, seq ASC"
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual(
                [(row["session_id"], row["seq"], row["ts_uptime_s"]) for row in rows],
                [(0, 104, 8207.6), (1, 89, 11640.7)],
            )

    def test_initialize_schema_moves_backfill_sessions_to_negative_range(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            connection = connect_sqlite(db_path)
            try:
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
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-28T17:26:31Z", "01", 3, 104, 8207.6, 18.53, 32.25, 0, -43, "", "BLE"),
                )
                connection.execute(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-25T16:58:57Z", "01", 3, 134, 6358.9, 25.0, 24.0, 0, -43, "", "CSV_BACKFILL"),
                )
                connection.execute(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-26T10:04:36Z", "01", 6, 134, 677.4, 24.0, 25.0, 0, -43, "", "CSV_BACKFILL"),
                )
                initialize_schema(connection)

                rows = connection.execute(
                    "SELECT source, session_id, seq FROM samples_raw WHERE pod_id = '01' ORDER BY source ASC, session_id ASC, seq ASC"
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual(
                [(row["source"], row["session_id"], row["seq"]) for row in rows],
                [("BLE", 3, 104), ("CSV_BACKFILL", -2, 134), ("CSV_BACKFILL", -1, 134)],
            )


class _FlakySqliteStorageWriter:
    def __init__(self, _db_path: Path, state: dict[str, int]) -> None:
        self._state = state

    def write_sample(self, **kwargs) -> SqliteWriteResult:
        self._state["attempts"] += 1
        if self._state["attempts"] == 1:
            raise IOError("simulated database lock")
        return SqliteWriteResult(inserted=True, duplicate=False)

    def write_link_snapshot(self, _snapshot: LinkSnapshot) -> None:
        return None

    def close(self) -> None:
        return None


class SqliteWriterPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_writer_pipeline_recovers_after_transient_insert_error(self) -> None:
        with TemporaryDirectory() as temp_dir:
            state = {"attempts": 0}

            def writer_factory(db_path: Path) -> _FlakySqliteStorageWriter:
                return _FlakySqliteStorageWriter(db_path, state)

            pipeline = SqliteWriterPipeline(
                db_path=Path(temp_dir) / "telemetry.sqlite",
                heartbeat_interval_s=60.0,
                reopen_delay_s=0.01,
                dependencies=SqliteWriterDependencies(storage_writer_factory=writer_factory),
            )
            pipeline.start()

            await pipeline.enqueue_sample(
                ts_pc_utc="2026-03-28T12:00:00Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=10.0,
                    temp_c=20.0,
                    rh_pct=40.0,
                    flags=0,
                ),
                rssi=-60,
                quality_flags=(),
            )
            await pipeline.stop()

            self.assertEqual(state["attempts"], 2)
            self.assertEqual(pipeline.metrics.write_errors, 1)
            self.assertEqual(pipeline.metrics.rows_written, 1)
            self.assertGreaterEqual(pipeline.metrics.commits, 1)


if __name__ == "__main__":
    unittest.main()
