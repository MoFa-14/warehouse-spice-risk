# File overview:
# - Responsibility: Provides regression coverage for watchdog behavior.
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
from datetime import timedelta
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "bleak" not in sys.modules:
    bleak_stub = types.ModuleType("bleak")

    # Class purpose: Provides the minimal BLE client test double needed for the
    #   watchdog regression module.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps the external dependency surface small so the
    #   watchdog tests can run without the real BLE library.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BleakClient:  # pragma: no cover - test stub only
        pass

    # Class purpose: Provides the minimal BLE scanner test double needed for
    #   the watchdog regression module.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps transport discovery behavior controllable so
    #   the watchdog tests can isolate reconnect logic from the BLE stack.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BleakScanner:  # pragma: no cover - test stub only
        # Test purpose: Provides the asynchronous discovery stub used by the
        #   watchdog tests.
        # - Project role: Belongs to the test and regression coverage and acts
        #   as a method on BleakScanner.
        # - Inputs: Arguments such as *args, **kwargs, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: Returns the computed value, structured record, or side
        #   effect defined by the implementation.
        # - Important decisions: Returns a deterministic empty discovery result
        #   so the watchdog path can be exercised without live radio state.
        # - Related flow: Executes runtime code under a controlled scenario and
        #   checks the expected branch, value, or data contract.
        @staticmethod
        async def discover(*args, **kwargs):
            return {}

        # Test purpose: Provides the asynchronous address lookup stub used by
        #   the watchdog tests.
        # - Project role: Belongs to the test and regression coverage and acts
        #   as a method on BleakScanner.
        # - Inputs: Arguments such as *args, **kwargs, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: Returns the computed value, structured record, or side
        #   effect defined by the implementation.
        # - Important decisions: Returns a deterministic miss so reconnect logic
        #   can be driven explicitly by the test fixture.
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
    #   watchdog test-side stub modules.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Mirrors only the attributes read by watchdog code
    #   so the regression path stays focused on timeout behavior.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.
    class BLEDevice:  # pragma: no cover - test stub only
        address = ""
        name = ""

    # Class purpose: Provides the minimal advertisement payload data shape
    #   required by the watchdog test-side stub modules.
    # - Project role: Belongs to the test and regression coverage and groups
    #   related state or behavior behind one explicit interface.
    # - Inputs: Initialization parameters and later method calls defined on the
    #   class.
    # - Outputs: Instances that hold state and expose related methods for later
    #   calls.
    # - Important decisions: Keeps only the attributes used by the gateway path
    #   under test so BLE dependency details do not dominate the scenario.
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
from gateway.utils.timeutils import utc_now
# Function purpose: Implements the noop handler step used by this subsystem.
# - Project role: Belongs to the test and regression coverage and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as *args, **kwargs, interpreted according to the rules
#   encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

async def _noop_handler(*args, **kwargs) -> None:
    return None
# Class purpose: Encapsulates the FakeClient responsibilities used by this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _FakeClient:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FakeClient.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self) -> None:
        self.is_connected = True
        self.stop_notify_calls = 0
        self.start_notify_calls = 0
        self.disconnect_calls = 0
    # Method purpose: Implements the stop notify step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FakeClient.
    # - Inputs: Arguments such as _uuid, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    async def stop_notify(self, _uuid: str) -> None:
        self.stop_notify_calls += 1
    # Method purpose: Implements the start notify step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FakeClient.
    # - Inputs: Arguments such as _uuid, _handler, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    async def start_notify(self, _uuid: str, _handler) -> None:
        self.start_notify_calls += 1
    # Method purpose: Implements the disconnect step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _FakeClient.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.is_connected = False
# Class purpose: Groups related regression checks for Watchdog behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class WatchdogTests(unittest.IsolatedAsyncioTestCase):
    # Method purpose: Builds session for the next stage of the project flow.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WatchdogTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns PodSession when the function completes successfully.
    # - Important decisions: The transformation rules here define how later code
    #   interprets the same data, so the shape of the output needs to stay
    #   stable and reproducible.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def _build_session(self) -> PodSession:
        settings = build_settings(
            firmware_config_path=None,
            log_dir="gateway/logs",
            addresses=None,
            scan_timeout_s=10.0,
            metrics_interval_s=30.0,
            rssi_poll_interval_s=30.0,
            temp_min_c=-20.0,
            temp_max_c=80.0,
            send_command=None,
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
    # Test purpose: Verifies that watchdog forces resubscribe after stall
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WatchdogTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_watchdog_forces_resubscribe_after_stall(self) -> None:
        session = self._build_session()
        fake_client = _FakeClient()
        now = utc_now()
        session._client = fake_client
        session.stats.connected = True
        session._connected_since_utc = now
        session._last_telemetry_time_utc = now

        stalled_now = now + timedelta(seconds=session._telemetry_stall_timeout_s() + 1.0)
        await session._telemetry_watchdog_tick(stalled_now)

        self.assertEqual(fake_client.stop_notify_calls, 1)
        self.assertEqual(fake_client.start_notify_calls, 1)
        self.assertEqual(fake_client.disconnect_calls, 0)
    # Test purpose: Verifies that watchdog reconnects after resubscribe does not
    #   restore telemetry behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WatchdogTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_watchdog_reconnects_after_resubscribe_does_not_restore_telemetry(self) -> None:
        session = self._build_session()
        fake_client = _FakeClient()
        now = utc_now()
        session._client = fake_client
        session.stats.connected = True
        session._connected_since_utc = now
        session._last_telemetry_time_utc = now

        await session._telemetry_watchdog_tick(now + timedelta(seconds=session._telemetry_stall_timeout_s() + 1.0))
        await session._telemetry_watchdog_tick(
            now
            + timedelta(
                seconds=session._telemetry_stall_timeout_s() + session._telemetry_reconnect_timeout_s() + 2.0
            )
        )

        self.assertEqual(fake_client.disconnect_calls, 1)
        self.assertTrue(session._disconnected_event.is_set())
    # Test purpose: Verifies that duplicate only telemetry does not reset
    #   watchdog progress timer behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WatchdogTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    async def test_duplicate_only_telemetry_does_not_reset_watchdog_progress_timer(self) -> None:
        session = self._build_session()
        fake_client = _FakeClient()
        now = utc_now()
        session._client = fake_client
        session.stats.connected = True
        session._connected_since_utc = now
        session._last_telemetry_time_utc = now
        session._seen_sequences.add(("01", 1))

        duplicate_payload = bytearray(b'{"pod_id":"01","seq":1,"ts_uptime_s":10.0,"temp_c":20.0,"rh_pct":40.0,"flags":0}')
        await session._handle_notification(None, duplicate_payload)

        await session._telemetry_watchdog_tick(now + timedelta(seconds=session._telemetry_stall_timeout_s() + 1.0))

        self.assertEqual(fake_client.stop_notify_calls, 1)
        self.assertEqual(fake_client.start_notify_calls, 1)
        self.assertEqual(fake_client.disconnect_calls, 0)


if __name__ == "__main__":
    unittest.main()
