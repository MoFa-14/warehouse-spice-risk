"""Gateway runtime configuration and CLI-facing settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from gateway.firmware_config_loader import FirmwareConfig, default_firmware_config_path, load_firmware_config
from gateway.storage.sqlite_db import resolve_db_path


@dataclass(frozen=True)
class ValidationSettings:
    """Telemetry value validation settings."""

    temp_min_c: float = -20.0
    temp_max_c: float = 80.0


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

    @property
    def device_name_scan_prefix(self) -> str:
        return self.ble_name_prefix or self.firmware.device_name_scan_prefix

    @property
    def sample_interval_s(self) -> int:
        return int(self.expected_sample_interval_s or self.firmware.sample_interval_s)


def normalize_address(address: str) -> str:
    """Normalize a BLE MAC string for comparisons and logging."""
    return address.strip().upper()


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
