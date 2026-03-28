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
}
_QUALITY_FLAG_NAMES_BY_VALUE = {
    int(member): name for name, member in _QUALITY_FLAG_MAP.items()
}


def quality_flags_to_mask(flags: Iterable[str]) -> int:
    """Encode flag names into a stable integer bitmask."""
    mask = QualityFlag.NONE
    for flag in flags:
        member = _QUALITY_FLAG_MAP.get(str(flag).strip().lower())
        if member is not None:
            mask |= member
    return int(mask)


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


def has_quality_flag(mask: int, flag: QualityFlag) -> bool:
    """Return whether a parsed bitmask contains the requested flag."""
    return bool(QualityFlag(mask) & flag)


def quality_mask_to_flags(mask: int | str | None) -> tuple[str, ...]:
    """Decode a stored quality mask back into stable flag names."""
    parsed_mask = parse_quality_mask(mask)
    resolved: list[str] = []
    for value, name in sorted(_QUALITY_FLAG_NAMES_BY_VALUE.items()):
        if parsed_mask & value:
            resolved.append(name)
    return tuple(resolved)
