"""Raw CSV parsing and sensor-value cleaning for Layer 3 preprocessing."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from gateway.storage.schema import QualityFlag, parse_quality_mask
from gateway.utils.timeutils import parse_utc_iso


@dataclass(frozen=True)
class RawSampleRow:
    """Typed view over one canonical raw telemetry CSV row."""

    ts_pc_utc: datetime
    pod_id: str
    seq: int
    ts_uptime_s: float
    temp_c: float | None
    rh_pct: float | None
    flags: int
    rssi: int | None
    quality_flags: int


@dataclass(frozen=True)
class CleanSampleRow:
    """Validated sensor row ready for resampling."""

    ts_pc_utc: datetime
    pod_id: str
    seq: int
    temp_c_clean: float | None
    rh_pct_clean: float | None
    quality_flags: int


def _coerce_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _coerce_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def read_raw_samples(path: Path) -> list[RawSampleRow]:
    """Load a canonical raw telemetry file into typed records."""
    rows: list[RawSampleRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                RawSampleRow(
                    ts_pc_utc=parse_utc_iso(row["ts_pc_utc"]),
                    pod_id=str(row["pod_id"]),
                    seq=int(row["seq"]),
                    ts_uptime_s=float(row["ts_uptime_s"]),
                    temp_c=_coerce_optional_float(row.get("temp_c")),
                    rh_pct=_coerce_optional_float(row.get("rh_pct")),
                    flags=int(row["flags"]),
                    rssi=_coerce_optional_int(row.get("rssi")),
                    quality_flags=parse_quality_mask(row.get("quality_flags")),
                )
            )
    rows.sort(key=lambda item: item.ts_pc_utc)
    return rows


def clean_samples(
    rows: Iterable[RawSampleRow],
    *,
    temp_min_c: float = -20.0,
    temp_max_c: float = 80.0,
) -> list[CleanSampleRow]:
    """Enforce numeric types and range checks without dropping audit context."""
    cleaned: list[CleanSampleRow] = []
    for row in rows:
        quality_mask = row.quality_flags
        temp_c = row.temp_c
        rh_pct = row.rh_pct

        if temp_c is None:
            quality_mask |= int(QualityFlag.TEMP_MISSING)
        elif not temp_min_c <= temp_c <= temp_max_c:
            quality_mask |= int(QualityFlag.TEMP_OUT_OF_RANGE)
            temp_c = None

        if rh_pct is None:
            quality_mask |= int(QualityFlag.RH_MISSING)
        elif not 0.0 <= rh_pct <= 100.0:
            quality_mask |= int(QualityFlag.RH_OUT_OF_RANGE)
            rh_pct = None

        cleaned.append(
            CleanSampleRow(
                ts_pc_utc=row.ts_pc_utc,
                pod_id=row.pod_id,
                seq=row.seq,
                temp_c_clean=temp_c,
                rh_pct_clean=rh_pct,
                quality_flags=quality_mask,
            )
        )
    cleaned.sort(key=lambda item: item.ts_pc_utc)
    return cleaned
