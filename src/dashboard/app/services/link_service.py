# File overview:
# - Responsibility: Services for dashboard data-quality and link-health views.
# - Project role: Builds route-ready view models, chart inputs, and interpretive
#   summaries from loaded data.
# - Main data or concerns: View models, chart series, classifications, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.
# - Why this matters: Keeping presentation logic here prevents routes and templates
#   from reimplementing analysis rules.

"""Services for dashboard data-quality and link-health views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.data_access.csv_reader import read_link_quality, read_raw_samples
from app.data_access.file_finder import discover_pod_ids, find_link_quality_files, find_raw_pod_files
from app.data_access.sqlite_reader import read_link_quality_sqlite, read_raw_samples_sqlite, sqlite_db_exists
# Class purpose: Row rendered on the health page.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class LinkHealthRow:
    """Row rendered on the health page."""

    pod_id: str
    latest_sample_ts: datetime | None
    latest_link_ts: datetime | None
    connected: bool | None
    last_rssi: float | None
    total_received: float | None
    total_missing: float | None
    total_duplicates: float | None
    missing_rate: float | None
# Function purpose: Build a health summary from raw and optional link-quality CSV
#   files.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def build_health_context(data_root: Path, *, db_path: Path | None = None) -> dict[str, object]:
    """Build a health summary from raw and optional link-quality CSV files."""
    data_root = Path(data_root)
    pod_ids = discover_pod_ids(data_root, db_path=db_path)
    link_frame = _load_link_frame(data_root, db_path=db_path)

    rows: list[LinkHealthRow] = []
    for pod_id in pod_ids:
        latest_raw = _latest_raw_row(data_root, pod_id, db_path=db_path)
        latest_link = _latest_link_row(link_frame, pod_id)
        rows.append(
            LinkHealthRow(
                pod_id=pod_id,
                latest_sample_ts=_timestamp_or_none(latest_raw, "ts_pc_utc"),
                latest_link_ts=_timestamp_or_none(latest_link, "ts_pc_utc"),
                connected=None if latest_link is None or pd.isna(latest_link.get("connected")) else bool(int(latest_link["connected"])),
                last_rssi=_float_or_none(latest_link.get("last_rssi") if latest_link is not None else None),
                total_received=_float_or_none(latest_link.get("total_received") if latest_link is not None else None),
                total_missing=_float_or_none(latest_link.get("total_missing") if latest_link is not None else None),
                total_duplicates=_float_or_none(latest_link.get("total_duplicates") if latest_link is not None else None),
                missing_rate=_float_or_none(latest_link.get("missing_rate") if latest_link is not None else None),
            )
        )

    latest_snapshot_ts = link_frame["ts_pc_utc"].max().to_pydatetime() if not link_frame.empty else None
    return {
        "rows": rows,
        "summary": {
            "pod_count": len(pod_ids),
            "link_rows": len(link_frame),
            "has_link_data": not link_frame.empty,
            "latest_snapshot_ts": latest_snapshot_ts,
        },
    }
# Function purpose: Retrieves the latest raw row available to the caller.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, db_path, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _latest_raw_row(data_root: Path, pod_id: str, *, db_path: Path | None = None):
    frame = _load_raw_frame(data_root, pod_id, db_path=db_path)
    if frame.empty:
        return None
    return frame.sort_values("ts_pc_utc").iloc[-1]
# Function purpose: Retrieves the latest link row available to the caller.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as link_frame, pod_id, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _latest_link_row(link_frame: pd.DataFrame, pod_id: str):
    if link_frame.empty:
        return None
    pod_frame = link_frame[link_frame["pod_id"] == pod_id]
    if pod_frame.empty:
        return None
    return pod_frame.sort_values("ts_pc_utc").iloc[-1]
# Function purpose: Implements the timestamp or none step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as row, column, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns datetime | None when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _timestamp_or_none(row, column: str) -> datetime | None:
    if row is None:
        return None
    value = row.get(column)
    if pd.isna(value):
        return None
    return value.to_pydatetime()
# Function purpose: Implements the float or none step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _float_or_none(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
# Function purpose: Loads raw frame into the structure expected by downstream code.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, db_path, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _load_raw_frame(data_root: Path, pod_id: str, *, db_path: Path | None = None) -> pd.DataFrame:
    if db_path is not None and sqlite_db_exists(db_path):
        return read_raw_samples_sqlite(db_path, pod_id=pod_id)
    return read_raw_samples(find_raw_pod_files(data_root, pod_id))
# Function purpose: Loads link frame into the structure expected by downstream code.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _load_link_frame(data_root: Path, *, db_path: Path | None = None) -> pd.DataFrame:
    if db_path is not None and sqlite_db_exists(db_path):
        return read_link_quality_sqlite(db_path)
    return read_link_quality(find_link_quality_files(data_root))
