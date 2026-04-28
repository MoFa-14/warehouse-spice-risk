# File overview:
# - Responsibility: Provides regression coverage for raw writer dedupe behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import csv
import sys
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.paths import build_storage_paths
from gateway.storage.raw_writer import RawTelemetryWriter
from gateway.storage.schema import RAW_SAMPLE_COLUMNS
# Class purpose: Groups related regression checks for RawWriterDedupe behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class RawWriterDedupeTests(unittest.TestCase):
    # Test purpose: Verifies that dedupe survives reopen and keeps single row
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on RawWriterDedupeTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_dedupe_survives_reopen_and_keeps_single_row(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            writer = RawTelemetryWriter(data_root)
            record = TelemetryRecord(
                pod_id="01",
                seq=7,
                ts_uptime_s=35.0,
                temp_c=22.5,
                rh_pct=51.2,
                flags=0,
            )

            first = writer.write_sample(
                ts_pc_utc="2026-03-25T15:42:01Z",
                record=record,
                rssi=-63,
                quality_flags=(),
            )
            self.assertTrue(first.inserted)
            self.assertFalse(first.duplicate)
            writer.close()

            reopened = RawTelemetryWriter(data_root)
            second = reopened.write_sample(
                ts_pc_utc="2026-03-25T15:42:01Z",
                record=record,
                rssi=-63,
                quality_flags=(),
            )
            self.assertFalse(second.inserted)
            self.assertTrue(second.duplicate)
            reopened.close()

            with first.path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual(reader.fieldnames, RAW_SAMPLE_COLUMNS)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["pod_id"], "01")
            self.assertEqual(rows[0]["seq"], "7")
            self.assertTrue(rows[0]["dew_point_c"])
    # Test purpose: Verifies that existing legacy day file is upgraded with dew
    #   point column behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on RawWriterDedupeTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_existing_legacy_day_file_is_upgraded_with_dew_point_column(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            storage_paths = build_storage_paths(data_root)
            legacy_path = storage_paths.raw_pod_day_path("01", date(2026, 3, 25))
            legacy_path.parent.mkdir(parents=True, exist_ok=True)

            with legacy_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "flags", "rssi", "quality_flags"])
                writer.writerow(["2026-03-25T15:42:01Z", "01", 1, 5.0, 20.0, 50.0, 0, -60, 0])

            writer = RawTelemetryWriter(data_root)
            writer.write_sample(
                ts_pc_utc="2026-03-25T15:42:06Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=2,
                    ts_uptime_s=10.0,
                    temp_c=21.0,
                    rh_pct=55.0,
                    flags=0,
                ),
                rssi=-58,
                quality_flags=(),
            )
            writer.close()

            with legacy_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual(reader.fieldnames, RAW_SAMPLE_COLUMNS)
            self.assertEqual(len(rows), 2)
            self.assertTrue(rows[0]["dew_point_c"])
            self.assertTrue(rows[1]["dew_point_c"])


if __name__ == "__main__":
    unittest.main()
