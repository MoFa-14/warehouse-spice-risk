# File overview:
# - Responsibility: GATT profile helpers shared by the gateway BLE client and tools.
# - Project role: Handles BLE discovery, connection, and GATT interaction for the
#   physical pod path.
# - Main data or concerns: BLE addresses, characteristics, notifications, and
#   connection state.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.
# - Why this matters: The physical pod path depends on this layer to convert radio
#   interaction into usable gateway events.

"""GATT profile helpers shared by the gateway BLE client and tools."""

from __future__ import annotations

from dataclasses import dataclass

from bleak import BleakClient

from gateway.firmware_config_loader import FirmwareConfig
# Class purpose: Single-source BLE UUID profile loaded from the firmware config.
# - Project role: Belongs to the gateway BLE transport layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

@dataclass(frozen=True)
class GattProfile:
    """Single-source BLE UUID profile loaded from the firmware config."""

    service_uuid: str
    telemetry_char_uuid: str
    control_char_uuid: str
    status_char_uuid: str
# Function purpose: Construct the BLE profile from the firmware source of truth.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as firmware, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns GattProfile when the function completes successfully.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

def profile_from_firmware(firmware: FirmwareConfig) -> GattProfile:
    """Construct the BLE profile from the firmware source of truth."""
    return GattProfile(
        service_uuid=firmware.service_uuid,
        telemetry_char_uuid=firmware.telemetry_char_uuid,
        control_char_uuid=firmware.control_char_uuid,
        status_char_uuid=firmware.status_char_uuid,
    )
# Function purpose: Ensure the expected service and characteristics exist after
#   connect.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as client, profile, interpreted according to the rules
#   encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

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
# Function purpose: Render discovered services and characteristics for debugging.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as client, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[str] when the function completes successfully.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

def iter_service_lines(client: BleakClient) -> list[str]:
    """Render discovered services and characteristics for debugging."""
    lines: list[str] = []
    for service in client.services:
        lines.append(f"SERVICE {service.uuid} {service.description}")
        for characteristic in service.characteristics:
            props = ",".join(characteristic.properties)
            lines.append(f"  CHAR {characteristic.uuid} [{props}]")
    return lines
# Function purpose: Read and decode the pod status payload.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as client, profile, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

async def read_status_text(client: BleakClient, profile: GattProfile) -> str:
    """Read and decode the pod status payload."""
    payload = await client.read_gatt_char(profile.status_char_uuid)
    return bytes(payload).decode("utf-8", errors="replace").strip()
# Function purpose: Write a UTF-8 control command to the pod.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as client, profile, command, interpreted according to the
#   rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

async def write_control_command(client: BleakClient, profile: GattProfile, command: str) -> None:
    """Write a UTF-8 control command to the pod."""
    await client.write_gatt_char(profile.control_char_uuid, command.encode("utf-8"), response=True)
