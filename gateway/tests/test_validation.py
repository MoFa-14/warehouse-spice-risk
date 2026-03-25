from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.firmware_config_loader import load_firmware_config
from gateway.protocol.decoder import TelemetryRecord
from gateway.protocol.validation import validate_telemetry


class ValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.firmware = load_firmware_config()

    def test_validation_flags_out_of_range_values(self) -> None:
        record = TelemetryRecord(
            pod_id="01",
            seq=10,
            ts_uptime_s=100.0,
            temp_c=120.0,
            rh_pct=120.0,
            flags=0,
        )
        result = validate_telemetry(
            record,
            temp_min_c=-20.0,
            temp_max_c=80.0,
            firmware=self.firmware,
        )
        self.assertEqual(result.quality_flags, ("temp_out_of_range", "rh_out_of_range"))

    def test_validation_flags_missing_values_and_sensor_error(self) -> None:
        record = TelemetryRecord(
            pod_id="01",
            seq=11,
            ts_uptime_s=101.0,
            temp_c=None,
            rh_pct=None,
            flags=self.firmware.flag_sensor_error,
        )
        result = validate_telemetry(
            record,
            temp_min_c=-20.0,
            temp_max_c=80.0,
            firmware=self.firmware,
        )
        self.assertEqual(result.quality_flags, ("temp_missing", "rh_missing", "sensor_error"))


if __name__ == "__main__":
    unittest.main()
