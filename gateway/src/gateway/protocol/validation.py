"""Telemetry validation and quality flag helpers."""

from __future__ import annotations

from dataclasses import dataclass

from gateway.firmware_config_loader import FirmwareConfig
from gateway.protocol.decoder import TelemetryRecord


@dataclass(frozen=True)
class ValidationResult:
    """Validation output stored alongside a decoded sample."""

    quality_flags: tuple[str, ...]


def validate_telemetry(
    record: TelemetryRecord,
    *,
    temp_min_c: float,
    temp_max_c: float,
    firmware: FirmwareConfig,
) -> ValidationResult:
    """Validate a telemetry record without throwing on bad sensor values."""
    quality_flags: list[str] = []

    if record.temp_c is None:
        quality_flags.append("temp_missing")
    elif not temp_min_c <= record.temp_c <= temp_max_c:
        quality_flags.append("temp_out_of_range")

    if record.rh_pct is None:
        quality_flags.append("rh_missing")
    elif not 0.0 <= record.rh_pct <= 100.0:
        quality_flags.append("rh_out_of_range")

    if firmware.flag_sensor_error and (record.flags & firmware.flag_sensor_error):
        quality_flags.append("sensor_error")
    if firmware.flag_low_batt and (record.flags & firmware.flag_low_batt):
        quality_flags.append("low_batt")

    return ValidationResult(quality_flags=tuple(dict.fromkeys(quality_flags)))


def format_quality_flags(flags: tuple[str, ...] | list[str]) -> str:
    """Render quality flags for CSV output."""
    return "|".join(flags)
