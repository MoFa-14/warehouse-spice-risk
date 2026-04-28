# File overview:
# - Responsibility: BLE discovery helpers for pod scanning and RSSI refreshes.
# - Project role: Handles BLE discovery, connection, and GATT interaction for the
#   physical pod path.
# - Main data or concerns: BLE addresses, characteristics, notifications, and
#   connection state.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.
# - Why this matters: The physical pod path depends on this layer to convert radio
#   interaction into usable gateway events.

"""BLE discovery helpers for pod scanning and RSSI refreshes."""

from __future__ import annotations

from dataclasses import dataclass

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

from gateway.config import normalize_address
# Class purpose: Single discovered pod candidate.
# - Project role: Belongs to the gateway BLE transport layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

@dataclass(frozen=True)
class ScanMatch:
    """Single discovered pod candidate."""

    address: str
    name: str
    rssi: int | None
    service_uuids: tuple[str, ...]
    ble_device: BLEDevice
# Function purpose: Implements the matches candidate step used by this subsystem.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as device, advertisement, addresses, name_prefix,
#   service_uuid, interpreted according to the rules encoded in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

def _matches_candidate(
    device: BLEDevice,
    advertisement: AdvertisementData,
    *,
    addresses: set[str],
    name_prefix: str,
    service_uuid: str,
) -> bool:
    address = normalize_address(device.address)
    if addresses:
        return address in addresses

    local_name = (advertisement.local_name or device.name or "").strip()
    advertised_services = {uuid.lower() for uuid in (advertisement.service_uuids or [])}
    return local_name.startswith(name_prefix) or service_uuid in advertised_services
# Function purpose: Scan for pods that match the configured prefix or explicit
#   addresses.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as timeout, name_prefix, service_uuid, addresses,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns list[ScanMatch] when the function completes successfully.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

async def discover_matches(
    *,
    timeout: float,
    name_prefix: str,
    service_uuid: str,
    addresses: tuple[str, ...] = (),
) -> list[ScanMatch]:
    """Scan for pods that match the configured prefix or explicit addresses."""
    address_filter = {normalize_address(value) for value in addresses}
    discovered = await BleakScanner.discover(timeout=timeout, return_adv=True)
    matches: dict[str, ScanMatch] = {}

    for device, advertisement in discovered.values():
        if not _matches_candidate(
            device,
            advertisement,
            addresses=address_filter,
            name_prefix=name_prefix,
            service_uuid=service_uuid,
        ):
            continue

        address = normalize_address(device.address)
        local_name = (advertisement.local_name or device.name or address).strip()
        match = ScanMatch(
            address=address,
            name=local_name,
            rssi=advertisement.rssi,
            service_uuids=tuple(uuid.lower() for uuid in (advertisement.service_uuids or [])),
            ble_device=device,
        )
        existing = matches.get(address)
        if existing is None or ((match.rssi or -999) > (existing.rssi or -999)):
            matches[address] = match

    return sorted(matches.values(), key=lambda item: item.address)
# Function purpose: Resolve a single pod by address, falling back to a direct
#   lookup.
# - Project role: Belongs to the gateway BLE transport layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as timeout, name_prefix, service_uuid, address,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns ScanMatch | None when the function completes successfully.
# - Important decisions: The physical pod path depends on this layer to convert
#   radio interaction into usable gateway events.
# - Related flow: Receives BLE-facing configuration or requests and passes transport
#   results to ingestion.

async def resolve_device(
    *,
    timeout: float,
    name_prefix: str,
    service_uuid: str,
    address: str,
) -> ScanMatch | None:
    """Resolve a single pod by address, falling back to a direct lookup."""
    normalized = normalize_address(address)
    matches = await discover_matches(
        timeout=timeout,
        name_prefix=name_prefix,
        service_uuid=service_uuid,
        addresses=(normalized,),
    )
    for match in matches:
        if match.address == normalized:
            return match

    device = await BleakScanner.find_device_by_address(normalized, timeout=timeout)
    if device is None:
        return None

    return ScanMatch(
        address=normalized,
        name=(device.name or normalized).strip(),
        rssi=None,
        service_uuids=(),
        ble_device=device,
    )
