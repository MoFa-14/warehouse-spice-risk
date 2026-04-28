# File overview:
# - Responsibility: Provides regression coverage for CSV import behavior.
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
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.storage.import_csv import import_csv_history
from gateway.storage.sqlite_reader import link_quality_in_range, samples_in_range
# Class purpose: Groups related regression checks for CsvImport behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class CsvImportTests(unittest.TestCase):
    # Test purpose: Verifies that import CSV history copies canonical and legacy
    #   rows once behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on CsvImportTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_import_csv_history_copies_canonical_and_legacy_rows_once(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            data_root = repo_root / "data"
            raw_pod_root = data_root / "raw" / "pods" / "01"
            raw_link_root = data_root / "raw" / "link_quality"
            legacy_logs_root = repo_root / "gateway" / "logs"

            raw_pod_root.mkdir(parents=True, exist_ok=True)
            raw_link_root.mkdir(parents=True, exist_ok=True)
            legacy_logs_root.mkdir(parents=True, exist_ok=True)

            _write_csv(
                raw_pod_root / "2026-03-25.csv",
                fieldnames=[
                    "ts_pc_utc",
                    "pod_id",
                    "seq",
                    "ts_uptime_s",
                    "temp_c",
                    "rh_pct",
                    "dew_point_c",
                    "flags",
                    "rssi",
                    "quality_flags",
                ],
                rows=[
                    {
                        "ts_pc_utc": "2026-03-25T12:00:10Z",
                        "pod_id": "01",
                        "seq": "2",
                        "ts_uptime_s": "20.0",
                        "temp_c": "24.1",
                        "rh_pct": "40.5",
                        "dew_point_c": "9.5",
                        "flags": "0",
                        "rssi": "-60",
                        "quality_flags": "0",
                    },
                    {
                        "ts_pc_utc": "2026-03-25T12:00:20Z",
                        "pod_id": "01",
                        "seq": "3",
                        "ts_uptime_s": "30.0",
                        "temp_c": "24.2",
                        "rh_pct": "40.8",
                        "dew_point_c": "9.7",
                        "flags": "0",
                        "rssi": "-60",
                        "quality_flags": "0",
                    },
                ],
            )
            _write_csv(
                legacy_logs_root / "samples.csv",
                fieldnames=[
                    "ts_pc_utc",
                    "pod_id",
                    "seq",
                    "ts_uptime_s",
                    "temp_c",
                    "rh_pct",
                    "dew_point_c",
                    "flags",
                    "rssi",
                    "quality_flags",
                ],
                rows=[
                    {
                        "ts_pc_utc": "2026-03-24T12:00:00Z",
                        "pod_id": "01",
                        "seq": "1",
                        "ts_uptime_s": "10.0",
                        "temp_c": "24.0",
                        "rh_pct": "40.0",
                        "dew_point_c": "9.2",
                        "flags": "0",
                        "rssi": "-61",
                        "quality_flags": "",
                    },
                    {
                        "ts_pc_utc": "2026-03-25T12:00:10Z",
                        "pod_id": "01",
                        "seq": "2",
                        "ts_uptime_s": "20.0",
                        "temp_c": "24.1",
                        "rh_pct": "40.5",
                        "dew_point_c": "9.5",
                        "flags": "0",
                        "rssi": "-60",
                        "quality_flags": "",
                    },
                ],
            )
            _write_csv(
                raw_link_root / "2026-03-25.csv",
                fieldnames=[
                    "ts_pc_utc",
                    "pod_id",
                    "connected",
                    "last_rssi",
                    "total_received",
                    "total_missing",
                    "total_duplicates",
                    "disconnect_count",
                    "reconnect_count",
                    "missing_rate",
                ],
                rows=[
                    {
                        "ts_pc_utc": "2026-03-25T12:00:30Z",
                        "pod_id": "01",
                        "connected": "1",
                        "last_rssi": "-60",
                        "total_received": "2",
                        "total_missing": "0",
                        "total_duplicates": "0",
                        "disconnect_count": "0",
                        "reconnect_count": "0",
                        "missing_rate": "0.0",
                    },
                ],
            )
            _write_csv(
                legacy_logs_root / "link_quality.csv",
                fieldnames=[
                    "ts_pc_utc",
                    "pod_id",
                    "connected",
                    "last_rssi",
                    "total_received",
                    "total_missing",
                    "total_duplicates",
                    "disconnect_count",
                    "reconnect_count",
                    "missing_rate",
                ],
                rows=[
                    {
                        "ts_pc_utc": "2026-03-24T12:00:30Z",
                        "pod_id": "SHT45-POD-01",
                        "connected": "true",
                        "last_rssi": "-61",
                        "total_received": "1",
                        "total_missing": "0",
                        "total_duplicates": "0",
                        "disconnect_count": "0",
                        "reconnect_count": "0",
                        "missing_rate": "0.0",
                    },
                    {
                        "ts_pc_utc": "2026-03-25T12:00:30Z",
                        "pod_id": "01",
                        "connected": "true",
                        "last_rssi": "-60",
                        "total_received": "2",
                        "total_missing": "0",
                        "total_duplicates": "0",
                        "disconnect_count": "0",
                        "reconnect_count": "0",
                        "missing_rate": "0.0",
                    },
                ],
            )

            db_path = data_root / "db" / "telemetry.sqlite"
            first = import_csv_history(data_root=data_root, db_path=db_path)
            second = import_csv_history(data_root=data_root, db_path=db_path)

            samples = samples_in_range(db_path=db_path, pod_id="01")
            link_rows = link_quality_in_range(db_path=db_path, pod_id="01")

            self.assertEqual(first.sample_rows_seen, 4)
            self.assertEqual(first.sample_rows_inserted, 3)
            self.assertEqual(first.sample_duplicates, 1)
            self.assertEqual(first.link_rows_seen, 3)
            self.assertEqual(first.link_rows_inserted, 2)
            self.assertEqual(first.link_duplicates, 1)

            self.assertEqual(second.sample_rows_inserted, 0)
            self.assertEqual(second.sample_duplicates, 4)
            self.assertEqual(second.link_rows_inserted, 0)
            self.assertEqual(second.link_duplicates, 3)

            self.assertEqual([row["seq"] for row in samples], [1, 2, 3])
            self.assertEqual(len(link_rows), 2)
            self.assertEqual({row["pod_id"] for row in link_rows}, {"01"})
# Function purpose: Writes CSV into the configured destination.
# - Project role: Belongs to the test and regression coverage and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as path, fieldnames, rows, interpreted according to the
#   rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

def _write_csv(path: Path, *, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
