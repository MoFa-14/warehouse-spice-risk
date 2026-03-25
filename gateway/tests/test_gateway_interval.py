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
from gateway.protocol.decoder import TelemetryRecord


async def _noop_handler(record: TelemetryRecord, quality_flags, stats, timestamp: str) -> None:
    return None


class GatewayCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_gateway_does_not_send_implicit_interval_command(self) -> None:
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
        session = PodSession(
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
        fake_client = object()

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            await session._maybe_send_command(fake_client)

        mock_write.assert_not_awaited()

    async def test_gateway_sends_explicit_control_command_only(self) -> None:
        settings = build_settings(
            firmware_config_path=None,
            log_dir="gateway/logs",
            addresses=None,
            scan_timeout_s=10.0,
            metrics_interval_s=30.0,
            rssi_poll_interval_s=30.0,
            temp_min_c=-20.0,
            temp_max_c=80.0,
            send_command="REQ_FROM_SEQ:123",
            use_cached_services=False,
        )
        session = PodSession(
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
        fake_client = object()

        with patch("gateway.ble.client.write_control_command", new=AsyncMock()) as mock_write:
            await session._maybe_send_command(fake_client)

        mock_write.assert_awaited_once_with(fake_client, session.profile, "REQ_FROM_SEQ:123")


if __name__ == "__main__":
    unittest.main()
