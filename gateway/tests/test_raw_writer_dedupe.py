from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.raw_writer import RawTelemetryWriter


class RawWriterDedupeTests(unittest.TestCase):
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
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["pod_id"], "01")
            self.assertEqual(rows[0]["seq"], "7")


if __name__ == "__main__":
    unittest.main()
