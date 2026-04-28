# File overview:
# - Responsibility: Provides regression coverage for resample grid behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.preprocess.clean import CleanSampleRow
from gateway.preprocess.resample import resample_day
# Class purpose: Groups related regression checks for ResampleGrid behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class ResampleGridTests(unittest.TestCase):
    # Test purpose: Verifies that resample builds uniform grid and uses last
    #   sample in bucket behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ResampleGridTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_resample_builds_uniform_grid_and_uses_last_sample_in_bucket(self) -> None:
        rows = [
            CleanSampleRow(
                ts_pc_utc=datetime(2026, 3, 25, 0, 0, 5, tzinfo=timezone.utc),
                pod_id="01",
                seq=1,
                temp_c_clean=20.0,
                rh_pct_clean=50.0,
                quality_flags=0,
            ),
            CleanSampleRow(
                ts_pc_utc=datetime(2026, 3, 25, 0, 0, 55, tzinfo=timezone.utc),
                pod_id="01",
                seq=2,
                temp_c_clean=21.0,
                rh_pct_clean=51.0,
                quality_flags=0,
            ),
            CleanSampleRow(
                ts_pc_utc=datetime(2026, 3, 25, 0, 2, 20, tzinfo=timezone.utc),
                pod_id="01",
                seq=3,
                temp_c_clean=22.0,
                rh_pct_clean=52.0,
                quality_flags=0,
            ),
        ]

        processed = resample_day(rows, day=date(2026, 3, 25), pod_id="01", interval_s=60)

        self.assertEqual(len(processed), 24 * 60)
        self.assertEqual(processed[0].ts_pc_utc, datetime(2026, 3, 25, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(processed[0].source_seq, 2)
        self.assertEqual(processed[0].missing, 0)
        self.assertEqual(processed[1].ts_pc_utc, datetime(2026, 3, 25, 0, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(processed[1].missing, 1)
        self.assertIsNone(processed[1].source_seq)
        self.assertEqual(processed[2].ts_pc_utc, datetime(2026, 3, 25, 0, 2, 0, tzinfo=timezone.utc))
        self.assertEqual(processed[2].source_seq, 3)
        self.assertEqual(processed[-1].ts_pc_utc, datetime(2026, 3, 25, 23, 59, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
