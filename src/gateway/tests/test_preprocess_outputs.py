# File overview:
# - Responsibility: Provides regression coverage for preprocess outputs behavior.
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

from gateway.preprocess.export import export_training_dataset, preprocess_day_file
from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.paths import build_storage_paths
from gateway.storage.raw_writer import RawTelemetryWriter
from gateway.storage.schema import PROCESSED_COLUMNS, TRAINING_DATASET_COLUMNS
# Class purpose: Groups related regression checks for PreprocessOutput behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class PreprocessOutputTests(unittest.TestCase):
    # Test purpose: Verifies that preprocess creates expected schema and missing
    #   markers behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PreprocessOutputTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_preprocess_creates_expected_schema_and_missing_markers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            writer = RawTelemetryWriter(data_root)
            writer.write_sample(
                ts_pc_utc="2026-03-25T00:00:05Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=5.0,
                    temp_c=20.0,
                    rh_pct=50.0,
                    flags=0,
                ),
                rssi=-60,
                quality_flags=(),
            )
            writer.write_sample(
                ts_pc_utc="2026-03-25T00:02:05Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=2,
                    ts_uptime_s=125.0,
                    temp_c=22.0,
                    rh_pct=52.0,
                    flags=0,
                ),
                rssi=-58,
                quality_flags=(),
            )
            raw_path = build_storage_paths(data_root).raw_pod_day_path("01", date(2026, 3, 25))
            writer.close()

            processed_path = preprocess_day_file(raw_path, data_root=data_root, interval_s=60)
            with processed_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual(reader.fieldnames, PROCESSED_COLUMNS)
            self.assertEqual(len(rows), 24 * 60)
            self.assertEqual(rows[0]["ts_pc_utc"], "2026-03-25T00:00:00Z")
            self.assertEqual(rows[0]["pod_id"], "01")
            self.assertEqual(rows[0]["missing"], "0")
            self.assertEqual(rows[0]["interpolated"], "0")
            self.assertTrue(rows[0]["dew_point_c"])
            self.assertEqual(rows[1]["ts_pc_utc"], "2026-03-25T00:01:00Z")
            self.assertEqual(rows[1]["missing"], "1")
            self.assertEqual(rows[1]["interpolated"], "0")
            self.assertEqual(rows[2]["ts_pc_utc"], "2026-03-25T00:02:00Z")
            self.assertEqual(rows[2]["missing"], "0")
            self.assertEqual(rows[2]["source_seq"], "2")
    # Test purpose: Verifies that export training concatenates processed rows
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PreprocessOutputTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_export_training_concatenates_processed_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            writer = RawTelemetryWriter(data_root)
            writer.write_sample(
                ts_pc_utc="2026-03-25T00:00:05Z",
                record=TelemetryRecord(
                    pod_id="01",
                    seq=1,
                    ts_uptime_s=5.0,
                    temp_c=20.0,
                    rh_pct=50.0,
                    flags=0,
                ),
                rssi=-60,
                quality_flags=(),
            )
            raw_path = build_storage_paths(data_root).raw_pod_day_path("01", date(2026, 3, 25))
            writer.close()

            preprocess_day_file(raw_path, data_root=data_root, interval_s=60)
            export_path = export_training_dataset(
                data_root=data_root,
                date_from=date(2026, 3, 25),
                date_to=date(2026, 3, 25),
            )
            with export_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual(reader.fieldnames, TRAINING_DATASET_COLUMNS)
            self.assertEqual(len(rows), 24 * 60)
            self.assertEqual(rows[0]["pod_id"], "01")
            self.assertIn("dew_point_c", rows[0])
            self.assertIn("missing", rows[0])


if __name__ == "__main__":
    unittest.main()
