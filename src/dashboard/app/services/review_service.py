# File overview:
# - Responsibility: Monitoring-review summaries for longer historical windows.
# - Project role: Builds route-ready view models, chart inputs, and interpretive
#   summaries from loaded data.
# - Main data or concerns: View models, chart series, classifications, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.
# - Why this matters: Keeping presentation logic here prevents routes and templates
#   from reimplementing analysis rules.

"""Monitoring-review summaries for longer historical windows.

The pod detail and prediction pages focus on a current or recent operational
view. This module serves a different purpose: it helps summarise what happened
across a chosen review window by combining telemetry, link-quality behaviour,
and stored forecast metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.data_access.csv_reader import read_link_quality, read_raw_samples
from app.data_access.file_finder import discover_pod_ids, find_link_quality_files, find_raw_pod_files
from app.data_access.forecast_reader import read_forecasts_in_window
from app.data_access.sqlite_reader import _connect, read_link_quality_sqlite, read_raw_samples_sqlite, sqlite_db_exists
from app.services.alerts_service import load_acknowledgements
from app.services.thresholds import classify_storage_conditions, level_definition
from app.services.timeseries_service import TimeWindow
# Class purpose: One per-pod monitoring summary row.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class MonitoringReviewRow:
    """One per-pod monitoring summary row."""

    pod_id: str
    sample_count: int
    excursion_count: int
    worst_level: int
    worst_level_label: str
    temp_trend_summary: str
    rh_trend_summary: str
    link_missing_samples: int
    duplicate_count: int
    reconnect_count: int
    missing_rate: float
    recommendation_event_count: int
    gateway_warning_count: int
# Function purpose: Build a per-pod review summary over a chosen historical window.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, window, db_path, pod_id, acks_file, now,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def build_monitoring_review_context(
    data_root: Path,
    *,
    window: TimeWindow,
    db_path: Path | None = None,
    pod_id: str | None = None,
    acks_file: Path | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build a per-pod review summary over a chosen historical window.

    This function is intended for retrospective monitoring analysis. It is less
    concerned with the most recent single reading and more concerned with counts
    of excursions, trends across the window, reconnect behaviour, and the
    presence of event-like forecast situations.
    """
    current_time = now or datetime.now(timezone.utc)
    data_root = Path(data_root)
    start_iso = _to_utc_iso(window.start)
    end_iso = _to_utc_iso(window.end)

    raw_frame = _load_raw_frame(data_root, db_path=db_path, pod_id=pod_id, window=window)
    link_frame = _load_link_frame(data_root, db_path=db_path, pod_id=pod_id, window=window)
    forecast_frame = read_forecasts_in_window(db_path, start_utc=start_iso, end_utc=end_iso, pod_id=pod_id)
    warning_counts = _gateway_warning_counts(db_path, start_utc=start_iso, end_utc=end_iso, pod_id=pod_id)

    discovered_pods = discover_pod_ids(data_root, db_path=db_path)
    pod_ids = sorted(
        {
            *(raw_frame["pod_id"].astype(str).tolist() if not raw_frame.empty else []),
            *(link_frame["pod_id"].astype(str).tolist() if not link_frame.empty else []),
            *(forecast_frame["pod_id"].astype(str).tolist() if not forecast_frame.empty else []),
            *(discovered_pods if pod_id is None else []),
            *([str(pod_id)] if pod_id else []),
        }
    )

    rows = [
        _build_row(
            pod_id=item,
            raw_frame=raw_frame[raw_frame["pod_id"] == item] if not raw_frame.empty else raw_frame,
            link_frame=link_frame[link_frame["pod_id"] == item] if not link_frame.empty else link_frame,
            forecast_frame=forecast_frame[forecast_frame["pod_id"] == item] if not forecast_frame.empty else forecast_frame,
            gateway_warning_count=warning_counts.get(item, 0),
        )
        for item in pod_ids
    ]
    rows = [row for row in rows if row is not None]

    total_excursions = sum(row.excursion_count for row in rows)
    total_recommendations = sum(row.recommendation_event_count for row in rows)
    worst_level = max((row.worst_level for row in rows), default=0)
    active_ack_count = len(load_acknowledgements(acks_file, now=current_time)) if acks_file is not None else 0

    return {
        "generated_at": current_time,
        "window": window,
        "scope_label": f"Pod {pod_id}" if pod_id else "All pods",
        "selected_pod_id": pod_id,
        "available_pods": discovered_pods,
        "rows": rows,
        "summary": {
            "pod_count": len(rows),
            "sample_count": sum(row.sample_count for row in rows),
            "excursion_count": total_excursions,
            "worst_level_label": level_definition(worst_level).short_label,
            "recommendation_event_count": total_recommendations,
            "active_acknowledgement_count": active_ack_count,
        },
    }
# Function purpose: Aggregate one pod's telemetry, link, and forecast evidence into
#   a row.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as pod_id, raw_frame, link_frame, forecast_frame,
#   gateway_warning_count, interpreted according to the rules encoded in the body
#   below.
# - Outputs: Returns MonitoringReviewRow | None when the function completes
#   successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _build_row(
    *,
    pod_id: str,
    raw_frame: pd.DataFrame,
    link_frame: pd.DataFrame,
    forecast_frame: pd.DataFrame,
    gateway_warning_count: int,
) -> MonitoringReviewRow | None:
    """Aggregate one pod's telemetry, link, and forecast evidence into a row."""
    if raw_frame.empty and link_frame.empty and forecast_frame.empty:
        return None

    levels = _classify_levels(raw_frame)
    worst_level = max(levels, default=0)
    link_summary = _link_summary(link_frame, raw_frame)
    recommendation_events = 0
    if not forecast_frame.empty:
        baseline_rows = forecast_frame[forecast_frame["scenario"] == "baseline"]
        recommendation_events = int(baseline_rows["event_detected"].sum())

    return MonitoringReviewRow(
        pod_id=pod_id,
        sample_count=int(len(raw_frame)),
        excursion_count=_count_excursions(levels),
        worst_level=worst_level,
        worst_level_label=level_definition(worst_level).short_label,
        temp_trend_summary=_trend_summary(raw_frame, value_column="temp_c", unit="C", stable_threshold=0.3),
        rh_trend_summary=_trend_summary(raw_frame, value_column="rh_pct", unit="%", stable_threshold=1.0),
        link_missing_samples=link_summary["missing"],
        duplicate_count=link_summary["duplicates"],
        reconnect_count=link_summary["reconnects"],
        missing_rate=link_summary["missing_rate"],
        recommendation_event_count=recommendation_events,
        gateway_warning_count=int(gateway_warning_count),
    )
# Function purpose: Loads raw frame into the structure expected by downstream code.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, window, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _load_raw_frame(data_root: Path, *, db_path: Path | None, pod_id: str | None, window: TimeWindow) -> pd.DataFrame:
    if db_path is not None and sqlite_db_exists(db_path):
        frame = read_raw_samples_sqlite(
            db_path,
            pod_id=pod_id,
            date_from=window.start.date(),
            date_to=window.end.date(),
        )
    else:
        if pod_id is not None:
            paths = find_raw_pod_files(data_root, pod_id, date_from=window.start.date(), date_to=window.end.date())
        else:
            paths: list[Path] = []
            for item in discover_pod_ids(data_root, db_path=db_path):
                paths.extend(find_raw_pod_files(data_root, item, date_from=window.start.date(), date_to=window.end.date()))
        frame = read_raw_samples(paths)
    return _filter_window(frame, window)
# Function purpose: Loads link frame into the structure expected by downstream code.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, window, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _load_link_frame(data_root: Path, *, db_path: Path | None, pod_id: str | None, window: TimeWindow) -> pd.DataFrame:
    if db_path is not None and sqlite_db_exists(db_path):
        frame = read_link_quality_sqlite(
            db_path,
            pod_id=pod_id,
            date_from=window.start.date(),
            date_to=window.end.date(),
        )
    else:
        frame = read_link_quality(find_link_quality_files(data_root, date_from=window.start.date(), date_to=window.end.date()))
        if pod_id is not None and not frame.empty:
            frame = frame[frame["pod_id"] == str(pod_id)].copy()
    return _filter_window(frame, window)
# Function purpose: Implements the filter window step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as frame, window, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _filter_window(frame: pd.DataFrame, window: TimeWindow) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame[(frame["ts_pc_utc"] >= pd.Timestamp(window.start)) & (frame["ts_pc_utc"] <= pd.Timestamp(window.end))].copy()
# Function purpose: Classifies levels according to the project rules below.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as raw_frame, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns list[int] when the function completes successfully.
# - Important decisions: The implementation encodes a project decision point that
#   later evaluation, storage, or dashboard logic depends on directly.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _classify_levels(raw_frame: pd.DataFrame) -> list[int]:
    levels: list[int] = []
    if raw_frame.empty:
        return levels
    for _, row in raw_frame.iterrows():
        status = classify_storage_conditions(_as_float(row.get("temp_c")), _as_float(row.get("rh_pct")))
        levels.append(0 if status is None else int(status.level))
    return levels
# Function purpose: Count distinct periods where conditions entered warning or
#   worse.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as levels, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _count_excursions(levels: list[int]) -> int:
    """Count distinct periods where conditions entered warning or worse."""
    count = 0
    in_excursion = False
    for level in levels:
        if level >= 2 and not in_excursion:
            count += 1
            in_excursion = True
        elif level < 2:
            in_excursion = False
    return count
# Function purpose: Express the start-to-end movement of one metric in concise
#   prose.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as raw_frame, value_column, unit, stable_threshold,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _trend_summary(raw_frame: pd.DataFrame, *, value_column: str, unit: str, stable_threshold: float) -> str:
    """Express the start-to-end movement of one metric in concise prose."""
    if raw_frame.empty or value_column not in raw_frame.columns:
        return "No data"
    series = pd.to_numeric(raw_frame[value_column], errors="coerce").dropna()
    if len(series) < 2:
        return "Insufficient data"
    delta = float(series.iloc[-1]) - float(series.iloc[0])
    if delta > stable_threshold:
        direction = "Rising"
    elif delta < -stable_threshold:
        direction = "Falling"
    else:
        direction = "Stable"
    return f"{direction} ({delta:+.2f} {unit} over window)"
# Function purpose: Summarise missing, duplicate, and reconnect behaviour for the
#   window.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as link_frame, raw_frame, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns dict[str, float | int] when the function completes
#   successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _link_summary(link_frame: pd.DataFrame, raw_frame: pd.DataFrame) -> dict[str, float | int]:
    """Summarise missing, duplicate, and reconnect behaviour for the window."""
    if not link_frame.empty:
        ordered = link_frame.sort_values("ts_pc_utc", kind="mergesort").reset_index(drop=True)
        first = ordered.iloc[0]
        last = ordered.iloc[-1]
        if len(ordered) == 1:
            missing = int(_as_float(last.get("total_missing")) or 0)
            duplicates = int(_as_float(last.get("total_duplicates")) or 0)
            reconnects = int(_as_float(last.get("reconnect_count")) or 0)
        else:
            missing = max(int(_as_float(last.get("total_missing")) or 0) - int(_as_float(first.get("total_missing")) or 0), 0)
            duplicates = max(int(_as_float(last.get("total_duplicates")) or 0) - int(_as_float(first.get("total_duplicates")) or 0), 0)
            reconnects = max(int(_as_float(last.get("reconnect_count")) or 0) - int(_as_float(first.get("reconnect_count")) or 0), 0)
        missing_rate = float(pd.to_numeric(ordered["missing_rate"], errors="coerce").dropna().mean() or 0.0)
        return {
            "missing": missing,
            "duplicates": duplicates,
            "reconnects": reconnects,
            "missing_rate": missing_rate,
        }

    return {
        "missing": _estimated_missing_from_samples(raw_frame),
        "duplicates": 0,
        "reconnects": 0,
        "missing_rate": 0.0 if raw_frame.empty else float(_estimated_missing_from_samples(raw_frame) / max(len(raw_frame), 1)),
    }
# Function purpose: Implements the estimated missing from samples step used by this
#   subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as raw_frame, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _estimated_missing_from_samples(raw_frame: pd.DataFrame) -> int:
    if raw_frame.empty or "seq" not in raw_frame.columns:
        return 0
    frame = raw_frame.sort_values(["ts_pc_utc", "seq"], kind="mergesort")
    missing = 0
    previous_seq: float | None = None
    previous_uptime: float | None = None
    for _, row in frame.iterrows():
        seq = _as_float(row.get("seq"))
        uptime = _as_float(row.get("ts_uptime_s"))
        if seq is None:
            continue
        if previous_seq is not None and previous_uptime is not None and uptime is not None:
            if seq > previous_seq + 1:
                missing += int(seq - previous_seq - 1)
            elif seq < previous_seq and uptime < previous_uptime:
                previous_seq = None
                previous_uptime = None
        previous_seq = seq
        previous_uptime = uptime
    return missing
# Function purpose: Implements the gateway warning counts step used by this
#   subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as db_path, start_utc, end_utc, pod_id, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns dict[str, int] when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _gateway_warning_counts(
    db_path: Path | None,
    *,
    start_utc: str,
    end_utc: str,
    pod_id: str | None,
) -> dict[str, int]:
    if db_path is None or not sqlite_db_exists(db_path):
        return {}

    query = """
        SELECT pod_id, COUNT(*) AS warning_count
        FROM gateway_events
        WHERE ts_pc_utc >= ?
          AND ts_pc_utc < ?
          AND level IN ('warning', 'error', 'critical')
    """
    parameters: list[object] = [start_utc, end_utc]
    if pod_id is not None:
        query += " AND pod_id = ?"
        parameters.append(str(pod_id))
    query += " GROUP BY pod_id"

    connection = _connect(db_path)
    try:
        try:
            rows = connection.execute(query, tuple(parameters)).fetchall()
        except Exception:
            return {}
    finally:
        connection.close()
    return {str(row["pod_id"]): int(row["warning_count"]) for row in rows if row["pod_id"] is not None}
# Function purpose: Implements the as float step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _as_float(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
# Function purpose: Implements the to UTC iso step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _to_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
