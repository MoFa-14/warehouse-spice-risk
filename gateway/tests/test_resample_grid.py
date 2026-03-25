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


class ResampleGridTests(unittest.TestCase):
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
