"""Services for latest per-pod dashboard readings."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.data_access.csv_reader import read_processed_samples, read_raw_samples
from app.data_access.file_finder import discover_pod_ids, find_processed_pod_files, find_raw_pod_files
from app.services.thresholds import ClassificationResult, classify_storage_conditions


@dataclass(frozen=True)
class PodLatestReading:
    """Latest dashboard-ready reading for one pod."""

    pod_id: str
    ts_pc_utc: datetime
    temp_c: float | None
    rh_pct: float | None
    dew_point_c: float | None
    data_source: str
    status: ClassificationResult | None


def discover_dashboard_pods(data_root: Path) -> list[str]:
    """Expose pod discovery for routes and navigation."""
    return discover_pod_ids(Path(data_root))


def get_latest_pod_readings(data_root: Path) -> list[PodLatestReading]:
    """Return latest readings for every discovered pod."""
    readings: list[PodLatestReading] = []
    for pod_id in discover_dashboard_pods(data_root):
        reading = get_latest_pod_reading(data_root, pod_id)
        if reading is not None:
            readings.append(reading)
    readings.sort(key=lambda item: item.pod_id)
    return readings


def get_latest_pod_reading(data_root: Path, pod_id: str) -> PodLatestReading | None:
    """Return the preferred latest reading for a single pod."""
    raw_files = find_raw_pod_files(Path(data_root), pod_id)
    if not raw_files:
        return None

    processed_files = find_processed_pod_files(Path(data_root), pod_id)
    processed_frame = read_processed_samples(processed_files[-3:]) if processed_files else pd.DataFrame()
    raw_frame = read_raw_samples(raw_files[-3:])

    processed_candidate = _latest_processed_row(processed_frame)
    raw_candidate = _latest_raw_row(raw_frame)

    if processed_candidate is not None:
        temp_c = _optional_float(processed_candidate.get("temp_c_clean"))
        rh_pct = _optional_float(processed_candidate.get("rh_pct_clean"))
        dew_point_c = _optional_float(processed_candidate.get("dew_point_c"))
        timestamp = processed_candidate["ts_pc_utc"]
        status = classify_storage_conditions(temp_c, rh_pct)
        return PodLatestReading(
            pod_id=pod_id,
            ts_pc_utc=timestamp.to_pydatetime(),
            temp_c=temp_c,
            rh_pct=rh_pct,
            dew_point_c=dew_point_c,
            data_source="processed",
            status=status,
        )

    if raw_candidate is None:
        return None

    temp_c = _optional_float(raw_candidate.get("temp_c"))
    rh_pct = _optional_float(raw_candidate.get("rh_pct"))
    timestamp = raw_candidate["ts_pc_utc"]
    status = classify_storage_conditions(temp_c, rh_pct)
    return PodLatestReading(
        pod_id=pod_id,
        ts_pc_utc=timestamp.to_pydatetime(),
        temp_c=temp_c,
        rh_pct=rh_pct,
        dew_point_c=None,
        data_source="raw",
        status=status,
    )


def _latest_processed_row(frame: pd.DataFrame):
    if frame.empty:
        return None
    candidate = frame.copy()
    candidate["missing"] = pd.to_numeric(candidate["missing"], errors="coerce") if "missing" in candidate else 1
    candidate = candidate[
        (candidate["missing"].fillna(1) == 0)
        | candidate["temp_c_clean"].notna()
        | candidate["rh_pct_clean"].notna()
    ]
    if candidate.empty:
        return None
    return candidate.sort_values("ts_pc_utc").iloc[-1]


def _latest_raw_row(frame: pd.DataFrame):
    if frame.empty:
        return None
    return frame.sort_values("ts_pc_utc").iloc[-1]


def _optional_float(value) -> float | None:
    if pd.isna(value):
        return None
    return float(value)
