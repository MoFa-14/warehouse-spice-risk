# File overview:
# - Responsibility: Provides regression coverage for validation behavior.
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

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.firmware_config_loader import load_firmware_config
from gateway.protocol.decoder import TelemetryRecord
from gateway.protocol.validation import validate_telemetry
# Class purpose: Groups related regression checks for Validation behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class ValidationTests(unittest.TestCase):
    # Method purpose: Implements the setUp step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ValidationTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def setUp(self) -> None:
        self.firmware = load_firmware_config()
    # Test purpose: Verifies that validation flags out of range values behaves
    #   as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ValidationTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that validation flags missing values and sensor
    #   error behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ValidationTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
