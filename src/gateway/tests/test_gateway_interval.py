# File overview:
# - Responsibility: Provides regression coverage for gateway interval behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "bleak" not in sys.modules:
    bleak_stub = types.ModuleType("bleak")

    # Class purpose: Provides the minimal BLE client test double needed for this
    #   regression module.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps the external dependency surface small so the
    #   gateway interval tests can run without the real BLE stack.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BleakClient:  # pragma: no cover - test stub only
        pass

    # Class purpose: Provides the minimal BLE scanner test double needed for
    #   this regression module.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps transport discovery behavior controllable so
    #   interval tests can isolate gateway timing logic from the BLE library.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BleakScanner:  # pragma: no cover - test stub only
        # Test purpose: Provides the asynchronous discovery stub used by the
        #   interval tests.
        # - Project role: Belongs to the test and regression coverage and acts
        #   as a method on BleakScanner.
        # - Inputs: Arguments such as *args, **kwargs, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: Returns the computed value, structured record, or side
        #   effect defined by the implementation.
        # - Important decisions: Returns a deterministic empty discovery result
        #   so the test controls the transport preconditions explicitly.
        # - Related flow: Executes runtime code under a controlled scenario and
        #   checks the expected branch, value, or data contract.
        @staticmethod
        async def discover(*args, **kwargs):
            return {}

        # Test purpose: Provides the asynchronous address lookup stub used by
        #   the interval tests.
        # - Project role: Belongs to the test and regression coverage and acts
        #   as a method on BleakScanner.
        # - Inputs: Arguments such as *args, **kwargs, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: Returns the computed value, structured record, or side
        #   effect defined by the implementation.
        # - Important decisions: Returns a deterministic miss so the test can
        #   decide when a device should or should not appear.
        # - Related flow: Executes runtime code under a controlled scenario and
        #   checks the expected branch, value, or data contract.
        @staticmethod
        async def find_device_by_address(*args, **kwargs):
            return None

    bleak_stub.BleakClient = BleakClient
    bleak_stub.BleakScanner = BleakScanner
    sys.modules["bleak"] = bleak_stub

    backends_stub = types.ModuleType("bleak.backends")
    device_stub = types.ModuleType("bleak.backends.device")
    scanner_stub = types.ModuleType("bleak.backends.scanner")

    # Class purpose: Provides the minimal BLE device data shape required by the
    #   test-side stub modules.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Mirrors only the attributes touched by gateway
    #   interval logic so the test stays focused on the target path.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BLEDevice:  # pragma: no cover - test stub only
        address = ""
        name = ""

    # Class purpose: Provides the minimal advertisement payload data shape
    #   required by the test-side stub modules.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps only the fields read by the gateway code so
    #   transport dependencies do not dominate the regression scenario.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class AdvertisementData:  # pragma: no cover - test stub only
        local_name = ""
        rssi = None
        service_uuids = ()

    device_stub.BLEDevice = BLEDevice
    scanner_stub.AdvertisementData = AdvertisementData
    sys.modules["bleak.backends"] = backends_stub
    sys.modules["bleak.backends.device"] = device_stub
    sys.modules["bleak.backends.scanner"] = scanner_stub

from gateway.ble.client import PodSession, PodTarget
from gateway.ble.gatt import GattProfile
from gateway.config import build_settings
from gateway.protocol.decoder import StatusRecord, TelemetryRecord
# Function purpose: Implements the noop handler step used by this subsystem.
# - Project role: Belongs to the test and regression coverage and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as record, quality_flags, stats, timestamp, interpreted
#   according to the rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

async def _noop_handler(record: TelemetryRecord, quality_flags, stats, timestamp: str) -> None:
    return None
# Class purpose: Groups related regression checks for GatewayCommand behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class GatewayCommandTests(unittest.IsolatedAsyncioTestCase):
    # Method purpose: Builds session for the next stage of the project flow.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on GatewayCommandTests.
    # - Inputs: Arguments such as send_command, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns PodSession when the function completes successfully.
    # - Important decisions: The transformation rules here define how later code
    #   interprets the same data, so the shape of the output needs to stay
    #   stable and reproducible.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def _build_session(self, *, send_command: str | None = None) -> PodSession:
        settings = build_settings(
            firmware_config_path=None,
            log_dir="gateway/logs",
            addresses=None,
            scan_timeout_s=10.0,
            metrics_interval_s=30.0,
            rssi_poll_interval_s=30.0,
            temp_min_c=-20.0,
            temp_max_c=80.0,
            send_command=send_command,
            use_cached_services=False,
        )
        return PodSession(
            target=PodTarget(address="AA:BB:CC:DD:EE:FF", name="SHT45-POD-01"),
            settings=settings,
            profile=GattProfile(
                service_uuid=settings.firmware.service_uuid,
                telemetry_char_uuid=settings.firmware.telemetry_char_uuid,
                control_char_uuid=settings.firmware.control_char_uuid,
                status_char_uuid=settings.firmware.status_char_uuid,
            ),
            sample_handler=_noop_handler,
        )
    # Test purpose: Verifies that gateway does not send control without command
    #   or mismatch behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on GatewayCommandTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_gateway_does_not_send_control_without_command_or_mismatch(self) -> None:
        session = self._build_session()
        fake_client = object()
        status = StatusRecord(firmware_version="0.1.0", last_error=0, sample_interval_s=60)

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            command_sent = await session._maybe_send_command(fake_client)
            interval_updated = await session._maybe_enforce_sample_interval(fake_client, status)

        self.assertFalse(command_sent)
        self.assertFalse(interval_updated)
        mock_write.assert_not_awaited()
    # Test purpose: Verifies that gateway sends explicit control command only
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on GatewayCommandTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_gateway_sends_explicit_control_command_only(self) -> None:
        session = self._build_session(send_command="REQ_FROM_SEQ:123")
        fake_client = object()

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            command_sent = await session._maybe_send_command(fake_client)

        self.assertTrue(command_sent)
        mock_write.assert_awaited_once_with(fake_client, session.profile, "REQ_FROM_SEQ:123")
    # Test purpose: Verifies that gateway enforces sixty second interval when
    #   pod reports ten behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on GatewayCommandTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_gateway_enforces_sixty_second_interval_when_pod_reports_ten(self) -> None:
        session = self._build_session()
        fake_client = object()
        status = StatusRecord(firmware_version="0.1.0", last_error=0, sample_interval_s=10)

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            interval_updated = await session._maybe_enforce_sample_interval(fake_client, status)

        self.assertTrue(interval_updated)
        mock_write.assert_awaited_once_with(fake_client, session.profile, "SET_INTERVAL:60")
    # Test purpose: Verifies that gateway does not double send when user
    #   explicitly sets interval behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on GatewayCommandTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_gateway_does_not_double_send_when_user_explicitly_sets_interval(self) -> None:
        session = self._build_session(send_command="SET_INTERVAL:60")
        fake_client = object()
        status = StatusRecord(firmware_version="0.1.0", last_error=0, sample_interval_s=10)

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            interval_updated = await session._maybe_enforce_sample_interval(fake_client, status)

        self.assertFalse(interval_updated)
        mock_write.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
