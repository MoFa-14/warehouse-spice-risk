"""GATT profile helpers shared by the gateway BLE client and tools."""

from __future__ import annotations

from dataclasses import dataclass

from bleak import BleakClient

from gateway.firmware_config_loader import FirmwareConfig


@dataclass(frozen=True)
class GattProfile:
    """Single-source BLE UUID profile loaded from the firmware config."""

    service_uuid: str
    telemetry_char_uuid: str
    control_char_uuid: str
    status_char_uuid: str


def profile_from_firmware(firmware: FirmwareConfig) -> GattProfile:
    """Construct the BLE profile from the firmware source of truth."""
    return GattProfile(
        service_uuid=firmware.service_uuid,
        telemetry_char_uuid=firmware.telemetry_char_uuid,
        control_char_uuid=firmware.control_char_uuid,
        status_char_uuid=firmware.status_char_uuid,
    )


def ensure_profile_present(client: BleakClient, profile: GattProfile) -> None:
    """Ensure the expected service and characteristics exist after connect."""
    service = client.services.get_service(profile.service_uuid)
    if service is None:
        raise RuntimeError(f"Custom service {profile.service_uuid} was not discovered.")

    missing = [
        uuid
        for uuid in (
            profile.telemetry_char_uuid,
            profile.control_char_uuid,
            profile.status_char_uuid,
        )
        if client.services.get_characteristic(uuid) is None
    ]
    if missing:
        raise RuntimeError(f"Missing expected characteristic(s): {', '.join(missing)}")


def iter_service_lines(client: BleakClient) -> list[str]:
    """Render discovered services and characteristics for debugging."""
    lines: list[str] = []
    for service in client.services:
        lines.append(f"SERVICE {service.uuid} {service.description}")
        for characteristic in service.characteristics:
            props = ",".join(characteristic.properties)
            lines.append(f"  CHAR {characteristic.uuid} [{props}]")
    return lines


async def read_status_text(client: BleakClient, profile: GattProfile) -> str:
    """Read and decode the pod status payload."""
    payload = await client.read_gatt_char(profile.status_char_uuid)
    return bytes(payload).decode("utf-8", errors="replace").strip()


async def write_control_command(client: BleakClient, profile: GattProfile, command: str) -> None:
    """Write a UTF-8 control command to the pod."""
    await client.write_gatt_char(profile.control_char_uuid, command.encode("utf-8"), response=True)
