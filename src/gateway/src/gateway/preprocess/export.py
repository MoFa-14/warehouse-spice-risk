"""Offline preprocessing and dataset export commands for Layer 3."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Iterable

from gateway.preprocess.clean import clean_samples, read_raw_samples
from gateway.preprocess.resample import ProcessedRow, resample_day
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.schema import PROCESSED_COLUMNS, TRAINING_DATASET_COLUMNS
from gateway.utils.timeutils import utc_now_iso


def preprocess_day_file(
    raw_path: Path,
    *,
    data_root: Path | str | None = None,
    interval_s: int = 60,
    interpolate: bool = False,
    max_gap_minutes: int = 5,
    temp_min_c: float = -20.0,
    temp_max_c: float = 80.0,
) -> Path:
    """Build one processed per-day CSV from one canonical raw day file."""
    pod_id = raw_path.parent.name
    day = date.fromisoformat(raw_path.stem)
    storage_paths = build_storage_paths(data_root)
    storage_paths.ensure_base_dirs()

    raw_rows = read_raw_samples(raw_path)
    clean_rows = clean_samples(raw_rows, temp_min_c=temp_min_c, temp_max_c=temp_max_c)
    processed_rows = resample_day(
        clean_rows,
        day=day,
        pod_id=pod_id,
        interval_s=interval_s,
        interpolate=interpolate,
        max_gap_minutes=max_gap_minutes,
    )

    output_path = storage_paths.processed_pod_day_path(pod_id, day)
    _write_processed_rows(output_path, processed_rows)
    return output_path


def preprocess_date_range(
    *,
    data_root: Path | str | None = None,
    pod_ids: Iterable[str] | None = None,
    date_from: date,
    date_to: date,
    interval_s: int = 60,
    interpolate: bool = False,
    max_gap_minutes: int = 5,
    temp_min_c: float = -20.0,
    temp_max_c: float = 80.0,
) -> list[Path]:
    """Process every matching raw day file in a date range."""
    storage_paths = build_storage_paths(data_root)
    outputs: list[Path] = []
    for raw_path in iter_raw_day_files(storage_paths, pod_ids=pod_ids, date_from=date_from, date_to=date_to):
        outputs.append(
            preprocess_day_file(
                raw_path,
                data_root=storage_paths.root,
                interval_s=interval_s,
                interpolate=interpolate,
                max_gap_minutes=max_gap_minutes,
                temp_min_c=temp_min_c,
                temp_max_c=temp_max_c,
            )
        )
    return outputs


def export_training_dataset(
    *,
    data_root: Path | str | None = None,
    date_from: date,
    date_to: date,
    out_path: Path | str | None = None,
) -> Path:
    """Concatenate processed daily CSV files into one training-ready export."""
    storage_paths = build_storage_paths(data_root)
    storage_paths.ensure_base_dirs()
    destination = Path(out_path) if out_path is not None else storage_paths.training_export_path()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRAINING_DATASET_COLUMNS)
        writer.writeheader()
        for processed_path in iter_processed_day_files(storage_paths, date_from=date_from, date_to=date_to):
            with processed_path.open("r", encoding="utf-8", newline="") as processed_handle:
                for row in csv.DictReader(processed_handle):
                    writer.writerow({column: row.get(column, "") for column in TRAINING_DATASET_COLUMNS})
    return destination


def iter_raw_day_files(
    storage_paths: StoragePaths,
    *,
    pod_ids: Iterable[str] | None,
    date_from: date,
    date_to: date,
) -> list[Path]:
    """List canonical raw day files that fall inside the requested window."""
    selected_pods = set(pod_ids or [])
    files: list[Path] = []
    if not storage_paths.raw_pods_root.exists():
        return files

    for pod_dir in sorted(path for path in storage_paths.raw_pods_root.iterdir() if path.is_dir()):
        if selected_pods and pod_dir.name not in selected_pods:
            continue
        for day_file in sorted(pod_dir.glob("*.csv")):
            try:
                day = date.fromisoformat(day_file.stem)
            except ValueError:
                continue
            if date_from <= day <= date_to:
                files.append(day_file)
    return files


def iter_processed_day_files(
    storage_paths: StoragePaths,
    *,
    date_from: date,
    date_to: date,
) -> list[Path]:
    """List processed day files within a requested export window."""
    files: list[Path] = []
    if not storage_paths.processed_pods_root.exists():
        return files

    for pod_dir in sorted(path for path in storage_paths.processed_pods_root.iterdir() if path.is_dir()):
        for day_file in sorted(pod_dir.glob("*_processed.csv")):
            stem = day_file.stem.removesuffix("_processed")
            try:
                day = date.fromisoformat(stem)
            except ValueError:
                continue
            if date_from <= day <= date_to:
                files.append(day_file)
    return files


def _write_processed_rows(path: Path, rows: list[ProcessedRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROCESSED_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_processed_row_to_csv(row))


def _processed_row_to_csv(row: ProcessedRow) -> dict[str, object]:
    return {
        "ts_pc_utc": utc_now_iso(row.ts_pc_utc),
        "pod_id": row.pod_id,
        "temp_c_clean": "" if row.temp_c_clean is None else f"{row.temp_c_clean:.6f}",
        "rh_pct_clean": "" if row.rh_pct_clean is None else f"{row.rh_pct_clean:.6f}",
        "dew_point_c": "" if row.dew_point_c is None else f"{row.dew_point_c:.6f}",
        "missing": row.missing,
        "interpolated": row.interpolated,
        "source_seq": "" if row.source_seq is None else row.source_seq,
    }
