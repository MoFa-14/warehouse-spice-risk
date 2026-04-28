# File overview:
# - Responsibility: Raw CSV parsing and sensor-value cleaning for Layer 3
#   preprocessing.
# - Project role: Cleans, resamples, derives, or exports telemetry into
#   analysis-ready forms.
# - Main data or concerns: Time-series points, derived psychrometric variables, and
#   resampled grids.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.
# - Why this matters: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.

"""Raw CSV parsing and sensor-value cleaning for Layer 3 preprocessing."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from gateway.storage.schema import QualityFlag, parse_quality_mask
from gateway.utils.timeutils import parse_utc_iso
# Class purpose: Typed view over one canonical raw telemetry CSV row.
# - Project role: Belongs to the gateway preprocessing layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

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
# Class purpose: Validated sensor row ready for resampling.
# - Project role: Belongs to the gateway preprocessing layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

@dataclass(frozen=True)
class CleanSampleRow:
    """Validated sensor row ready for resampling."""

    ts_pc_utc: datetime
    pod_id: str
    seq: int
    temp_c_clean: float | None
    rh_pct_clean: float | None
    quality_flags: int
# Function purpose: Coerces optional float into the type expected by downstream
#   code.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Rejects malformed or incompatible input early so later code
#   can assume typed values rather than repeating the same checks.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _coerce_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)
# Function purpose: Coerces optional int into the type expected by downstream code.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int | None when the function completes successfully.
# - Important decisions: Rejects malformed or incompatible input early so later code
#   can assume typed values rather than repeating the same checks.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _coerce_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)
# Function purpose: Load a canonical raw telemetry file into typed records.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as path, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[RawSampleRow] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

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
# Function purpose: Enforce numeric types and range checks without dropping audit
#   context.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as rows, temp_min_c, temp_max_c, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns list[CleanSampleRow] when the function completes successfully.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

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
