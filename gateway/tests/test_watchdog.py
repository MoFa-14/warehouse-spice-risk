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

    class BleakClient:  # pragma: no cover - test stub only
        pass

    class BleakScanner:  # pragma: no cover - test stub only
        @staticmethod
        async def discover(*args, **kwargs):
            return {}

        @staticmethod
        async def find_device_by_address(*args, **kwargs):
            return None

    bleak_stub.BleakClient = BleakClient
    bleak_stub.BleakScanner = BleakScanner
    sys.modules["bleak"] = bleak_stub

    backends_stub = types.ModuleType("bleak.backends")
    device_stub = types.ModuleType("bleak.backends.device")
    scanner_stub = types.ModuleType("bleak.backends.scanner")

    class BLEDevice:  # pragma: no cover - test stub only
        address = ""
        name = ""

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


async def _noop_handler(*args, **kwargs) -> None:
    return None


class _FakeClient:
    def __init__(self) -> None:
        self.is_connected = True
        self.stop_notify_calls = 0
        self.start_notify_calls = 0
        self.disconnect_calls = 0

    async def stop_notify(self, _uuid: str) -> None:
        self.stop_notify_calls += 1

    async def start_notify(self, _uuid: str, _handler) -> None:
        self.start_notify_calls += 1

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.is_connected = False


class WatchdogTests(unittest.IsolatedAsyncioTestCase):
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


if __name__ == "__main__":
    unittest.main()
