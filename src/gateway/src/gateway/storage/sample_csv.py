# File overview:
# - Responsibility: Shared helpers for raw and compatibility sample CSV files.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Shared helpers for raw and compatibility sample CSV files."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Sequence

from gateway.preprocess.dewpoint import dew_point_c
from gateway.protocol.decoder import TelemetryRecord
# Function purpose: Build one sample CSV row with dew point derived from temperature
#   and humidity.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as ts_pc_utc, record, rssi, quality_flags, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def build_sample_row(
    *,
    ts_pc_utc: str,
    record: TelemetryRecord,
    rssi: int | None,
    quality_flags: Any,
) -> dict[str, object]:
    """Build one sample CSV row with dew point derived from temperature and humidity."""
    dew_point = dew_point_c(record.temp_c, record.rh_pct)
    return {
        "ts_pc_utc": ts_pc_utc,
        "pod_id": record.pod_id,
        "seq": record.seq,
        "ts_uptime_s": record.ts_uptime_s,
        "temp_c": record.temp_c,
        "rh_pct": record.rh_pct,
        "dew_point_c": "" if dew_point is None else f"{dew_point:.6f}",
        "flags": record.flags,
        "rssi": rssi,
        "quality_flags": quality_flags,
    }
# Function purpose: Upgrade legacy sample files in place so new dew-point writes
#   stay well-formed.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as path, fieldnames, interpreted according to the rules
#   encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def ensure_sample_csv_schema(path: Path, fieldnames: Sequence[str]) -> None:
    """Upgrade legacy sample files in place so new dew-point writes stay well-formed."""
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        current_fields = list(reader.fieldnames or [])
        if current_fields == list(fieldnames):
            return
        rows = [_normalize_sample_row(row, fieldnames) for row in reader]

    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    temp_path.replace(path)
# Function purpose: Normalizes sample row into the subsystem's stable
#   representation.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as row, fieldnames, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _normalize_sample_row(row: dict[str, Any], fieldnames: Sequence[str]) -> dict[str, object]:
    normalized = {field: row.get(field, "") for field in fieldnames}
    dew_text = str(row.get("dew_point_c", "")).strip()
    if dew_text:
        normalized["dew_point_c"] = dew_text
        return normalized

    dew_point = dew_point_c(_coerce_optional_float(row.get("temp_c")), _coerce_optional_float(row.get("rh_pct")))
    normalized["dew_point_c"] = "" if dew_point is None else f"{dew_point:.6f}"
    return normalized
# Function purpose: Coerces optional float into the type expected by downstream
#   code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Rejects malformed or incompatible input early so later code
#   can assume typed values rather than repeating the same checks.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)
