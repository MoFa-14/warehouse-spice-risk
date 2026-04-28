# File overview:
# - Responsibility: Provides regression coverage for diagnostics behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.link.diagnostics import diagnostics_in_range
from gateway.storage.sqlite_db import connect_sqlite, init_db
# Class purpose: Groups related regression checks for DiagnosticsSummary behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class DiagnosticsSummaryTests(unittest.TestCase):
    # Test purpose: Verifies that diagnostics summary reports link resend and
    #   drift metrics behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DiagnosticsSummaryTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_diagnostics_summary_reports_link_resend_and_drift_metrics(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "telemetry.sqlite"
            init_db(db_path)
            connection = connect_sqlite(db_path)
            try:
                connection.executemany(
                    """
                    INSERT INTO samples_raw (
                        ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("2026-03-28T12:00:00Z", "01", 0, 1, 10.0, 20.0, 45.0, 0, -50, "", "BLE"),
                        ("2026-03-28T12:01:00Z", "01", 0, 2, 70.0, 20.2, 45.2, 0, -49, "", "BLE"),
                        ("2026-03-28T12:05:00Z", "01", 0, 3, 130.0, 20.4, 45.4, 0, -48, "time_sync_anomaly", "BLE"),
                        ("2026-03-28T12:06:00Z", "01", 1, 1, 5.0, 20.1, 45.1, 0, -47, "", "BLE"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO link_quality (
                        ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                        total_duplicates, disconnect_count, reconnect_count, missing_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("2026-03-28T12:00:00Z", "01", 1, -50, 1, 0, 0, 0, 0, 0.0),
                        ("2026-03-28T12:06:00Z", "01", 1, -47, 4, 2, 1, 0, 1, 0.3333),
                    ],
                )
                connection.executemany(
                    "INSERT INTO gateway_events (ts_pc_utc, level, pod_id, message) VALUES (?, ?, ?, ?)",
                    [
                        ("2026-03-28T12:02:00Z", "warning", "01", "resend_request from_seq=3"),
                        ("2026-03-28T12:05:00Z", "warning", "01", "time_sync_anomaly drift_s=120.0 seq=3"),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            summaries = diagnostics_in_range(
                db_path=db_path,
                start_utc="2026-03-28T11:59:00Z",
                end_utc="2026-03-28T12:10:00Z",
            )

            self.assertEqual(len(summaries), 1)
            summary = summaries[0]
            self.assertEqual(summary.pod_id, "01")
            self.assertEqual(summary.sample_count, 4)
            self.assertEqual(summary.session_count, 2)
            self.assertEqual(summary.missing_samples, 2)
            self.assertEqual(summary.duplicate_count, 1)
            self.assertEqual(summary.reconnect_count, 1)
            self.assertEqual(summary.resend_request_count, 1)
            self.assertGreaterEqual(summary.drift_anomaly_count, 1)
            self.assertGreater(summary.max_abs_drift_s, 0.0)
            self.assertEqual(summary.min_rssi, -50)
            self.assertEqual(summary.max_rssi, -47)


if __name__ == "__main__":
    unittest.main()
