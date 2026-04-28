# File overview:
# - Responsibility: Gateway runtime configuration and CLI-facing settings.
# - Project role: Defines configuration and top-level runtime wiring for live
#   gateway operation.
# - Main data or concerns: Runtime options, configuration values, and top-level
#   service wiring.
# - Related flow: Connects lower gateway subsystems into runnable entry points.
# - Why this matters: Top-level runtime wiring determines how the live ingestion and
#   storage path is assembled.

"""Gateway runtime configuration and CLI-facing settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from gateway.firmware_config_loader import FirmwareConfig, default_firmware_config_path, load_firmware_config
from gateway.storage.sqlite_db import resolve_db_path
# Class purpose: Telemetry value validation settings.
# - Project role: Belongs to the gateway runtime layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

@dataclass(frozen=True)
class ValidationSettings:
    """Telemetry value validation settings."""

    temp_min_c: float = -20.0
    temp_max_c: float = 80.0
# Class purpose: Resolved runtime settings for the gateway process.
# - Project role: Belongs to the gateway runtime layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Top-level runtime wiring determines how the live ingestion
#   and storage path is assembled.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

@dataclass(frozen=True)
class GatewaySettings:
    """Resolved runtime settings for the gateway process."""

    firmware: FirmwareConfig
    log_dir: Path
    storage_backend: str
    db_path: Path
    addresses: tuple[str, ...]
    scan_timeout_s: float = 10.0
    metrics_interval_s: float = 30.0
    rssi_poll_interval_s: float = 30.0
    validation: ValidationSettings = ValidationSettings()
    send_command: str | None = None
    use_cached_services: bool = False
    ble_name_prefix: str | None = None
    expected_sample_interval_s: int | None = None
    # Method purpose: Implements the device name scan prefix step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway runtime layer and acts as a method
    #   on GatewaySettings.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns str when the function completes successfully.
    # - Important decisions: Top-level runtime wiring determines how the live
    #   ingestion and storage path is assembled.
    # - Related flow: Connects lower gateway subsystems into runnable entry
    #   points.

    @property
    def device_name_scan_prefix(self) -> str:
        return self.ble_name_prefix or self.firmware.device_name_scan_prefix
    # Method purpose: Implements the sample interval s step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway runtime layer and acts as a method
    #   on GatewaySettings.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns int when the function completes successfully.
    # - Important decisions: Top-level runtime wiring determines how the live
    #   ingestion and storage path is assembled.
    # - Related flow: Connects lower gateway subsystems into runnable entry
    #   points.

    @property
    def sample_interval_s(self) -> int:
        return int(self.expected_sample_interval_s or self.firmware.sample_interval_s)
# Function purpose: Normalize a BLE MAC string for comparisons and logging.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as address, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def normalize_address(address: str) -> str:
    """Normalize a BLE MAC string for comparisons and logging."""
    return address.strip().upper()
# Function purpose: Parse repeated or comma-separated CLI address values.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns tuple[str, ...] when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def parse_addresses(values: list[str] | None) -> tuple[str, ...]:
    """Parse repeated or comma-separated CLI address values."""
    if not values:
        return ()

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for part in raw_value.split(","):
            candidate = normalize_address(part)
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
    return tuple(normalized)
# Function purpose: Load the authoritative firmware config and merge user overrides.
# - Project role: Belongs to the gateway runtime layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as firmware_config_path, log_dir, addresses,
#   scan_timeout_s, metrics_interval_s, rssi_poll_interval_s, temp_min_c,
#   temp_max_c, send_command, use_cached_services, storage_backend, db_path,
#   ble_name_prefix, expected_sample_interval_s, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns GatewaySettings when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Connects lower gateway subsystems into runnable entry points.

def build_settings(
    *,
    firmware_config_path: str | None,
    log_dir: str,
    addresses: list[str] | None,
    scan_timeout_s: float,
    metrics_interval_s: float,
    rssi_poll_interval_s: float,
    temp_min_c: float,
    temp_max_c: float,
    send_command: str | None,
    use_cached_services: bool,
    storage_backend: str = "sqlite",
    db_path: str | None = None,
    ble_name_prefix: str | None = None,
    expected_sample_interval_s: int | None = None,
) -> GatewaySettings:
    """Load the authoritative firmware config and merge user overrides."""
    config_path = Path(firmware_config_path) if firmware_config_path else default_firmware_config_path()
    firmware = load_firmware_config(config_path)

    return GatewaySettings(
        firmware=firmware,
        log_dir=Path(log_dir),
        storage_backend=storage_backend.strip().lower(),
        db_path=resolve_db_path(db_path),
        addresses=parse_addresses(addresses),
        scan_timeout_s=scan_timeout_s,
        metrics_interval_s=metrics_interval_s,
        rssi_poll_interval_s=rssi_poll_interval_s,
        validation=ValidationSettings(temp_min_c=temp_min_c, temp_max_c=temp_max_c),
        send_command=send_command.strip() if send_command else None,
        use_cached_services=use_cached_services,
        ble_name_prefix=ble_name_prefix.strip() if ble_name_prefix else None,
        expected_sample_interval_s=expected_sample_interval_s,
    )
