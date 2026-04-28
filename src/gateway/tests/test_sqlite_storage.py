# File overview:
# - Responsibility: Provides regression coverage for SQLite storage behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

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
# Class purpose: Groups related regression checks for SqliteStorage behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class SqliteStorageTests(unittest.TestCase):
    # Test purpose: Verifies that schema creation creates required tables
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that insert or ignore dedupes same pod and seq
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that range query returns expected rows behaves as
    #   expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that sequence restart reuses seq in new SQLite
    #   session behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that soft reload sequence drop with higher uptime
    #   opens new session behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that small sequence restart with higher uptime
    #   opens new session behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_small_sequence_restart_with_higher_uptime_opens_new_session(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            writer = SqliteStorageWriter(db_path)
            writer.write_sample(
                ts_pc_utc="2026-03-28T19:01:00Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=4,
                    ts_uptime_s=12846.4,
                    temp_c=18.43,
                    rh_pct=33.22,
                    flags=0,
                ),
                rssi=-43,
                quality_flags=(),
                source="BLE",
            )
            restarted = writer.write_sample(
                ts_pc_utc="2026-03-28T19:06:20Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=2,
                    ts_uptime_s=12864.0,
                    temp_c=18.48,
                    rh_pct=33.32,
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
                [(0, 4, 12846.4), (1, 2, 12864.0)],
            )
    # Test purpose: Verifies that initialize schema moves backfill sessions to
    #   negative range behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteStorageTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
# Class purpose: Encapsulates the FlakySqliteStorageWriter responsibilities used by
#   this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _FlakySqliteStorageWriter:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakySqliteStorageWriter.
    # - Inputs: Arguments such as _db_path, state, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self, _db_path: Path, state: dict[str, int]) -> None:
        self._state = state
    # Method purpose: Writes sample into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakySqliteStorageWriter.
    # - Inputs: Arguments such as **kwargs, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns SqliteWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_sample(self, **kwargs) -> SqliteWriteResult:
        self._state["attempts"] += 1
        if self._state["attempts"] == 1:
            raise IOError("simulated database lock")
        return SqliteWriteResult(inserted=True, duplicate=False)
    # Method purpose: Writes link snapshot into the configured destination.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakySqliteStorageWriter.
    # - Inputs: Arguments such as _snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def write_link_snapshot(self, _snapshot: LinkSnapshot) -> None:
        return None
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FlakySqliteStorageWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def close(self) -> None:
        return None
# Class purpose: Groups related regression checks for SqliteWriterPipeline behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class SqliteWriterPipelineTests(unittest.IsolatedAsyncioTestCase):
    # Test purpose: Verifies that writer pipeline recovers after transient
    #   insert error behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on SqliteWriterPipelineTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
