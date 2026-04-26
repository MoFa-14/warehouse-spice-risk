"""Services that build the dashboard's notion of "latest pod state".

The dashboard overview and pod-detail pages need one concise, consistent answer
to the question "what is the latest reading for this pod right now?". This file
implements that answer by loading the most recent raw and processed candidates,
applying dashboard calibration rules, and selecting the best available reading.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.data_access.csv_reader import read_processed_samples, read_raw_samples
from app.data_access.file_finder import discover_pod_ids, find_processed_pod_files, find_raw_pod_files
from app.data_access.sqlite_reader import read_raw_samples_sqlite, sqlite_db_exists
from app.services.telemetry_adjustments import apply_calibration, load_adjustments
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
    has_measurement: bool
    last_complete_ts_pc_utc: datetime | None


@dataclass(frozen=True)
class _ReadingCandidate:
    pod_id: str
    ts_pc_utc: datetime
    temp_c: float | None
    rh_pct: float | None
    dew_point_c: float | None
    data_source: str

    @property
    def has_measurement(self) -> bool:
        return any(value is not None for value in (self.temp_c, self.rh_pct, self.dew_point_c))


def discover_dashboard_pods(data_root: Path, *, db_path: Path | None = None) -> list[str]:
    """Expose consistent pod discovery for routes and navigation."""
    return discover_pod_ids(Path(data_root), db_path=db_path)


def get_latest_pod_readings(
    data_root: Path,
    *,
    db_path: Path | None = None,
    adjustments_path: Path | None = None,
) -> list[PodLatestReading]:
    """Build the latest-reading card set for all known pods."""
    readings: list[PodLatestReading] = []
    adjustments = load_adjustments(adjustments_path)
    for pod_id in discover_dashboard_pods(data_root, db_path=db_path):
        reading = get_latest_pod_reading(data_root, pod_id, db_path=db_path, adjustments=adjustments)
        if reading is not None:
            readings.append(reading)
    readings.sort(key=lambda item: item.pod_id)
    return readings


def get_latest_pod_reading(
    data_root: Path,
    pod_id: str,
    *,
    db_path: Path | None = None,
    adjustments_path: Path | None = None,
    adjustments=None,
) -> PodLatestReading | None:
    """Return the preferred current reading for one pod.

    The function deliberately compares raw and processed candidates because the
    project contains both original measurements and cleaned derivatives. The
    dashboard should display the freshest trustworthy information without hiding
    the source it came from.
    """
    raw_frame = _load_raw_frame(Path(data_root), pod_id, db_path=db_path)
    processed_files = find_processed_pod_files(Path(data_root), pod_id)
    processed_frame = read_processed_samples(processed_files[-3:]) if processed_files else pd.DataFrame()
    resolved_adjustments = adjustments or load_adjustments(adjustments_path)
    raw_frame = _adjust_raw_frame(raw_frame, resolved_adjustments)
    processed_frame = _adjust_processed_frame(processed_frame, resolved_adjustments)

    current_candidates = [
        candidate
        for candidate in (
            _candidate_from_raw_row(_latest_row(raw_frame)),
            _candidate_from_processed_row(_latest_processed_row(processed_frame)),
        )
        if candidate is not None
    ]
    if not current_candidates:
        return None

    # When timestamps tie, processed data is preferred because it may include
    # repaired or smoothed values that are more presentation-ready.
    current = max(current_candidates, key=lambda item: (item.ts_pc_utc, item.data_source == "processed"))
    last_complete = _latest_complete_candidate(raw_frame, processed_frame)
    status = classify_storage_conditions(current.temp_c, current.rh_pct)

    return PodLatestReading(
        pod_id=current.pod_id,
        ts_pc_utc=current.ts_pc_utc,
        temp_c=current.temp_c,
        rh_pct=current.rh_pct,
        dew_point_c=current.dew_point_c,
        data_source=current.data_source,
        status=status,
        has_measurement=current.has_measurement,
        last_complete_ts_pc_utc=None if last_complete is None else last_complete.ts_pc_utc,
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
        | candidate["dew_point_c"].notna()
    ]
    if candidate.empty:
        return None
    return candidate.iloc[-1]


def _latest_row(frame: pd.DataFrame):
    if frame.empty:
        return None
    return frame.iloc[-1]


def _latest_complete_candidate(raw_frame: pd.DataFrame, processed_frame: pd.DataFrame) -> _ReadingCandidate | None:
    """Return the newest candidate that still has at least one real measurement."""
    candidates = [
        candidate
        for candidate in (
            _candidate_from_raw_row(_latest_measurement_row(raw_frame)),
            _candidate_from_processed_row(_latest_processed_row(processed_frame)),
        )
        if candidate is not None and candidate.has_measurement
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item.ts_pc_utc, item.data_source == "processed"))


def _latest_measurement_row(frame: pd.DataFrame):
    if frame.empty:
        return None
    candidate = frame[
        frame["temp_c"].notna()
        | frame["rh_pct"].notna()
        | frame["dew_point_c"].notna()
    ]
    if candidate.empty:
        return None
    return candidate.iloc[-1]


def _load_raw_frame(data_root: Path, pod_id: str, *, db_path: Path | None = None) -> pd.DataFrame:
    """Load recent raw telemetry from SQLite when possible, else CSV fallback."""
    if db_path is not None and sqlite_db_exists(db_path):
        return read_raw_samples_sqlite(db_path, pod_id=pod_id)
    raw_files = find_raw_pod_files(Path(data_root), pod_id)
    return read_raw_samples(raw_files[-3:]) if raw_files else pd.DataFrame()


def _adjust_raw_frame(frame: pd.DataFrame, adjustments) -> pd.DataFrame:
    if frame.empty:
        return frame
    return apply_calibration(frame, temp_column="temp_c", rh_column="rh_pct", adjustments=adjustments)


def _adjust_processed_frame(frame: pd.DataFrame, adjustments) -> pd.DataFrame:
    if frame.empty:
        return frame
    return apply_calibration(frame, temp_column="temp_c_clean", rh_column="rh_pct_clean", adjustments=adjustments)


def _candidate_from_raw_row(row) -> _ReadingCandidate | None:
    if row is None:
        return None
    return _ReadingCandidate(
        pod_id=str(row.get("pod_id", "")),
        ts_pc_utc=row["ts_pc_utc"].to_pydatetime(),
        temp_c=_optional_float(row.get("temp_c")),
        rh_pct=_optional_float(row.get("rh_pct")),
        dew_point_c=_optional_float(row.get("dew_point_c")),
        data_source="raw",
    )


def _candidate_from_processed_row(row) -> _ReadingCandidate | None:
    if row is None:
        return None
    return _ReadingCandidate(
        pod_id=str(row.get("pod_id", "")),
        ts_pc_utc=row["ts_pc_utc"].to_pydatetime(),
        temp_c=_optional_float(row.get("temp_c_clean")),
        rh_pct=_optional_float(row.get("rh_pct_clean")),
        dew_point_c=_optional_float(row.get("dew_point_c")),
        data_source="processed",
    )


def _optional_float(value) -> float | None:
    if pd.isna(value):
        return None
    return float(value)
