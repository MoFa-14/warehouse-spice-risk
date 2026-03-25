"""Services for dashboard data-quality and link-health views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from app.data_access.csv_reader import read_link_quality, read_raw_samples
from app.data_access.file_finder import discover_pod_ids, find_link_quality_files, find_raw_pod_files


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


def build_health_context(data_root: Path) -> dict[str, object]:
    """Build a health summary from raw and optional link-quality CSV files."""
    data_root = Path(data_root)
    pod_ids = discover_pod_ids(data_root)
    link_frame = read_link_quality(find_link_quality_files(data_root))

    rows: list[LinkHealthRow] = []
    for pod_id in pod_ids:
        latest_raw = _latest_raw_row(data_root, pod_id)
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


def _latest_raw_row(data_root: Path, pod_id: str):
    frame = read_raw_samples(find_raw_pod_files(data_root, pod_id))
    if frame.empty:
        return None
    return frame.sort_values("ts_pc_utc").iloc[-1]


def _latest_link_row(link_frame: pd.DataFrame, pod_id: str):
    if link_frame.empty:
        return None
    pod_frame = link_frame[link_frame["pod_id"] == pod_id]
    if pod_frame.empty:
        return None
    return pod_frame.sort_values("ts_pc_utc").iloc[-1]


def _timestamp_or_none(row, column: str) -> datetime | None:
    if row is None:
        return None
    value = row.get(column)
    if pd.isna(value):
        return None
    return value.to_pydatetime()


def _float_or_none(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
