# File overview:
# - Responsibility: Stable CSV schemas and quality-flag helpers for Layer 3 storage.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Stable CSV schemas and quality-flag helpers for Layer 3 storage."""

from __future__ import annotations

from enum import IntFlag
from typing import Iterable


RAW_SAMPLE_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "seq",
    "ts_uptime_s",
    "temp_c",
    "rh_pct",
    "dew_point_c",
    "flags",
    "rssi",
    "quality_flags",
]

LINK_QUALITY_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "connected",
    "last_rssi",
    "total_received",
    "total_missing",
    "total_duplicates",
    "disconnect_count",
    "reconnect_count",
    "missing_rate",
]

PROCESSED_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "temp_c_clean",
    "rh_pct_clean",
    "dew_point_c",
    "missing",
    "interpolated",
    "source_seq",
]

TRAINING_DATASET_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "temp_c_clean",
    "rh_pct_clean",
    "dew_point_c",
    "missing",
]
# Class purpose: Bitmask persisted in Layer 3 raw storage for audit-friendly replay.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class QualityFlag(IntFlag):
    """Bitmask persisted in Layer 3 raw storage for audit-friendly replay."""

    NONE = 0
    TEMP_MISSING = 1 << 0
    TEMP_OUT_OF_RANGE = 1 << 1
    RH_MISSING = 1 << 2
    RH_OUT_OF_RANGE = 1 << 3
    SENSOR_ERROR = 1 << 4
    LOW_BATT = 1 << 5
    SEQUENCE_RESET = 1 << 6
    SEQ_GAP = 1 << 7
    JSON_ERROR_FIXED = 1 << 8
    DUPLICATE = 1 << 9
    TIME_SYNC_ANOMALY = 1 << 10


_QUALITY_FLAG_MAP = {
    "temp_missing": QualityFlag.TEMP_MISSING,
    "temp_out_of_range": QualityFlag.TEMP_OUT_OF_RANGE,
    "rh_missing": QualityFlag.RH_MISSING,
    "rh_out_of_range": QualityFlag.RH_OUT_OF_RANGE,
    "sensor_error": QualityFlag.SENSOR_ERROR,
    "low_batt": QualityFlag.LOW_BATT,
    "sequence_reset": QualityFlag.SEQUENCE_RESET,
    "seq_gap": QualityFlag.SEQ_GAP,
    "json_error_fixed": QualityFlag.JSON_ERROR_FIXED,
    "duplicate": QualityFlag.DUPLICATE,
    "time_sync_anomaly": QualityFlag.TIME_SYNC_ANOMALY,
}
_QUALITY_FLAG_NAMES_BY_VALUE = {
    int(member): name for name, member in _QUALITY_FLAG_MAP.items()
}
# Function purpose: Encode flag names into a stable integer bitmask.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as flags, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def quality_flags_to_mask(flags: Iterable[str]) -> int:
    """Encode flag names into a stable integer bitmask."""
    mask = QualityFlag.NONE
    for flag in flags:
        member = _QUALITY_FLAG_MAP.get(str(flag).strip().lower())
        if member is not None:
            mask |= member
    return int(mask)
# Function purpose: Parse either a numeric mask or a legacy pipe-delimited flag
#   string.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def parse_quality_mask(value: int | str | None) -> int:
    """Parse either a numeric mask or a legacy pipe-delimited flag string."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value

    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        return quality_flags_to_mask(part for part in text.split("|") if part)
# Function purpose: Return whether a parsed bitmask contains the requested flag.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as mask, flag, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def has_quality_flag(mask: int, flag: QualityFlag) -> bool:
    """Return whether a parsed bitmask contains the requested flag."""
    return bool(QualityFlag(mask) & flag)
# Function purpose: Decode a stored quality mask back into stable flag names.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as mask, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns tuple[str, ...] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def quality_mask_to_flags(mask: int | str | None) -> tuple[str, ...]:
    """Decode a stored quality mask back into stable flag names."""
    parsed_mask = parse_quality_mask(mask)
    resolved: list[str] = []
    for value, name in sorted(_QUALITY_FLAG_NAMES_BY_VALUE.items()):
        if parsed_mask & value:
            resolved.append(name)
    return tuple(resolved)
