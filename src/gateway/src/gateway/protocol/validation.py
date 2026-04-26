"""Telemetry validation rules used immediately after decoding.

Decoding answers the question "can this payload be parsed?". Validation answers
the next question: "does this parsed telemetry look trustworthy enough to store
without losing the context that something may be wrong?".

The important design choice here is that validation does not discard records
just because a value is missing or suspicious. Instead it records quality flags
that later appear in storage, review pages, and debugging workflows.
"""

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
    """Convert decoded telemetry into a set of quality flags.

    This function is called before a sample enters durable storage. The result
    is intentionally non-destructive: a suspect value is flagged rather than
    silently discarded so the database preserves both the measurement attempt
    and the reason it may not be trustworthy.
    """
    quality_flags: list[str] = []

    # Temperature is checked against a project-level engineering envelope rather
    # than an arbitrary generic range. This helps catch obvious sensor faults or
    # decoding corruption while still allowing realistic warehouse variability.
    if record.temp_c is None:
        quality_flags.append("temp_missing")
    elif not temp_min_c <= record.temp_c <= temp_max_c:
        quality_flags.append("temp_out_of_range")

    # Relative humidity has a strong physical bound of 0-100%. Values outside
    # that interval are useful as diagnostics, but they should never be treated
    # as normal environmental behaviour.
    if record.rh_pct is None:
        quality_flags.append("rh_missing")
    elif not 0.0 <= record.rh_pct <= 100.0:
        quality_flags.append("rh_out_of_range")

    # Firmware-set bit flags are preserved as higher-level quality labels so the
    # rest of the Python stack does not need to know the original bit layout.
    if firmware.flag_sensor_error and (record.flags & firmware.flag_sensor_error):
        quality_flags.append("sensor_error")
    if firmware.flag_low_batt and (record.flags & firmware.flag_low_batt):
        quality_flags.append("low_batt")

    return ValidationResult(quality_flags=tuple(dict.fromkeys(quality_flags)))


def format_quality_flags(flags: tuple[str, ...] | list[str]) -> str:
    """Render quality flags into the storage-friendly pipe-delimited form."""
    return "|".join(flags)
