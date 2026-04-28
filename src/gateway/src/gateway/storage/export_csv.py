# File overview:
# - Responsibility: Export SQLite-backed telemetry into CSV files for reporting or
#   appendices.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Export SQLite-backed telemetry into CSV files for reporting or appendices."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Iterable

from gateway.preprocess.dewpoint import dew_point_c
from gateway.storage.paths import build_storage_paths
from gateway.storage.sqlite_reader import samples_in_range, utc_bounds_for_dates


EXPORT_COLUMNS = [
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
    "source",
]
# Function purpose: Export one pod's raw rows from SQLite into a CSV file.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as pod_id, date_from, date_to, out_path, db_path,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns Path when the function completes successfully.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def export_pod_csv(
    *,
    pod_id: str,
    date_from: date,
    date_to: date,
    out_path: Path | str | None = None,
    db_path: Path | str | None = None,
) -> Path:
    """Export one pod's raw rows from SQLite into a CSV file."""
    storage_paths = build_storage_paths()
    destination = Path(out_path) if out_path is not None else storage_paths.exports_root / f"pod{pod_id}.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)

    ts_from_utc, ts_to_utc = utc_bounds_for_dates(date_from, date_to)
    rows = samples_in_range(db_path=db_path, pod_id=pod_id, ts_from_utc=ts_from_utc, ts_to_utc=ts_to_utc)
    _write_rows(destination, rows)
    return destination
# Function purpose: Export one CSV per pod for the requested UTC date range.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as date_from, date_to, out_dir, db_path, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[Path] when the function completes successfully.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def export_all_pods_csv(
    *,
    date_from: date,
    date_to: date,
    out_dir: Path | str | None = None,
    db_path: Path | str | None = None,
) -> list[Path]:
    """Export one CSV per pod for the requested UTC date range."""
    storage_paths = build_storage_paths()
    destination_dir = Path(out_dir) if out_dir is not None else storage_paths.exports_root
    destination_dir.mkdir(parents=True, exist_ok=True)

    ts_from_utc, ts_to_utc = utc_bounds_for_dates(date_from, date_to)
    rows = samples_in_range(db_path=db_path, ts_from_utc=ts_from_utc, ts_to_utc=ts_to_utc)
    rows_by_pod: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        rows_by_pod.setdefault(str(row["pod_id"]), []).append(row)

    outputs: list[Path] = []
    for pod_id, pod_rows in sorted(rows_by_pod.items()):
        destination = destination_dir / f"pod{pod_id}.csv"
        _write_rows(destination, pod_rows)
        outputs.append(destination)
    return outputs
# Function purpose: Writes rows into the configured destination.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as path, rows, interpreted according to the rules encoded
#   in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _write_rows(path: Path, rows: Iterable[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_export_row(row))
# Function purpose: Exports row into the external format used by later tools.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as row, interpreted according to the rules encoded in the
#   body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _export_row(row: dict[str, object]) -> dict[str, object]:
    temp_c = _coerce_float(row.get("temp_c"))
    rh_pct = _coerce_float(row.get("rh_pct"))
    dew_point = dew_point_c(temp_c, rh_pct)
    return {
        "ts_pc_utc": row.get("ts_pc_utc", ""),
        "pod_id": row.get("pod_id", ""),
        "seq": row.get("seq", ""),
        "ts_uptime_s": row.get("ts_uptime_s", ""),
        "temp_c": "" if temp_c is None else temp_c,
        "rh_pct": "" if rh_pct is None else rh_pct,
        "dew_point_c": "" if dew_point is None else f"{dew_point:.6f}",
        "flags": row.get("flags", ""),
        "rssi": row.get("rssi", ""),
        "quality_flags": row.get("quality_flags", ""),
        "source": row.get("source", ""),
    }
# Function purpose: Coerces float into the type expected by downstream code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Rejects malformed or incompatible input early so later code
#   can assume typed values rather than repeating the same checks.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)
