from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.protocol.decoder import DecodeError, decode_status_payload, decode_telemetry_payload


class DecoderTests(unittest.TestCase):
    def test_decode_valid_status_payload(self) -> None:
        status = decode_status_payload("0.1.0-phase1,0,60")
        self.assertEqual(status.firmware_version, "0.1.0-phase1")
        self.assertEqual(status.last_error, 0)
        self.assertEqual(status.sample_interval_s, 60)

    def test_decode_valid_telemetry_payload(self) -> None:
        record = decode_telemetry_payload(
            '{"pod_id":"01","seq":7,"ts_uptime_s":12.5,"temp_c":21.34,"rh_pct":54.12,"flags":0}'
        )
        self.assertEqual(record.pod_id, "01")
        self.assertEqual(record.seq, 7)
        self.assertAlmostEqual(record.ts_uptime_s, 12.5)
        self.assertAlmostEqual(record.temp_c or 0.0, 21.34)
        self.assertAlmostEqual(record.rh_pct or 0.0, 54.12)
        self.assertEqual(record.flags, 0)

    def test_decode_rejects_missing_field(self) -> None:
        with self.assertRaises(DecodeError):
            decode_telemetry_payload('{"pod_id":"01","seq":7}')


if __name__ == "__main__":
    unittest.main()
