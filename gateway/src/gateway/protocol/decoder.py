"""Decode status and telemetry payloads from the pod BLE protocol."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping


class DecodeError(ValueError):
    """Raised when a BLE payload cannot be decoded into the expected shape."""


@dataclass(frozen=True)
class StatusRecord:
    """Parsed Status characteristic payload."""

    firmware_version: str
    last_error: int
    sample_interval_s: int


@dataclass(frozen=True)
class TelemetryRecord:
    """Parsed Telemetry notification payload."""

    pod_id: str
    seq: int
    ts_uptime_s: float
    temp_c: float | None
    rh_pct: float | None
    flags: int


def _coerce_int(value: Any, field_name: str) -> int:
    try:
        if isinstance(value, bool):
            raise TypeError(field_name)
        return int(value)
    except (TypeError, ValueError) as exc:
        raise DecodeError(f"{field_name} is not an integer: {value!r}") from exc


def _coerce_float(value: Any, field_name: str) -> float:
    try:
        if isinstance(value, bool):
            raise TypeError(field_name)
        return float(value)
    except (TypeError, ValueError) as exc:
        raise DecodeError(f"{field_name} is not a number: {value!r}") from exc


def _coerce_optional_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    return _coerce_float(value, field_name)


def decode_status_payload(payload: bytes | bytearray | str) -> StatusRecord:
    """Decode the pod status CSV payload."""
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else str(payload)
    parts = [part.strip() for part in text.strip().split(",")]
    if len(parts) != 3:
        raise DecodeError(f"Status payload must have 3 comma-separated fields, got: {text!r}")

    return StatusRecord(
        firmware_version=parts[0],
        last_error=_coerce_int(parts[1], "last_error"),
        sample_interval_s=_coerce_int(parts[2], "sample_interval_s"),
    )


def decode_telemetry_payload(payload: bytes | bytearray | str | Mapping[str, Any]) -> TelemetryRecord:
    """Decode a telemetry JSON object from bytes, text, or a parsed mapping."""
    if isinstance(payload, Mapping):
        raw_message = dict(payload)
    else:
        text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else str(payload)
        try:
            raw_message = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DecodeError(f"Telemetry payload is not valid JSON: {text!r}") from exc

    required_fields = ("pod_id", "seq", "ts_uptime_s", "temp_c", "rh_pct", "flags")
    missing = [name for name in required_fields if name not in raw_message]
    if missing:
        raise DecodeError(f"Telemetry payload is missing required field(s): {', '.join(missing)}")

    return TelemetryRecord(
        pod_id=str(raw_message["pod_id"]),
        seq=_coerce_int(raw_message["seq"], "seq"),
        ts_uptime_s=_coerce_float(raw_message["ts_uptime_s"], "ts_uptime_s"),
        temp_c=_coerce_optional_float(raw_message["temp_c"], "temp_c"),
        rh_pct=_coerce_optional_float(raw_message["rh_pct"], "rh_pct"),
        flags=_coerce_int(raw_message["flags"], "flags"),
    )
