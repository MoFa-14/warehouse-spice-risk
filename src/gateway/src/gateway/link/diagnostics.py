# File overview:
# - Responsibility: SQLite-backed per-pod diagnostics summaries for link and timing
#   health.
# - Project role: Computes communication quality, sequence gaps, and timing
#   diagnostics.
# - Main data or concerns: Sequence counters, timestamps, connectivity statistics,
#   and missing-rate metrics.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.
# - Why this matters: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.

"""SQLite-backed per-pod diagnostics summaries for link and timing health.

Forecast quality depends on more than the model alone. If a pod is reconnecting
frequently, losing sequences, or showing strong timing drift, those factors are
important context for interpreting both missing telemetry and later forecast
behaviour. This module turns stored telemetry and link records into explicit
diagnostic summaries.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from gateway.link.time_alignment import AlignmentState, align_sample
from gateway.storage.sqlite_db import connect_sqlite, resolve_db_path
from gateway.utils.timeutils import utc_now_iso
# Class purpose: Per-pod diagnostics summary over a chosen UTC window.
# - Project role: Belongs to the gateway link-diagnostics layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

@dataclass(frozen=True)
class PodDiagnosticsSummary:
    """Per-pod diagnostics summary over a chosen UTC window."""

    pod_id: str
    sample_count: int
    session_count: int
    avg_rssi: float | None
    min_rssi: int | None
    max_rssi: int | None
    missing_samples: int
    duplicate_count: int
    reconnect_count: int
    resend_request_count: int
    drift_anomaly_count: int
    max_abs_drift_s: float
    latest_estimated_sample_time_utc: str | None
# Function purpose: Build a serialisable diagnostics report over a recent time
#   window.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, hours, pod_ids, end_utc, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def build_diagnostics_summary(
    *,
    db_path: Path | str | None,
    hours: float = 24.0,
    pod_ids: list[str] | None = None,
    end_utc: datetime | None = None,
) -> dict[str, object]:
    """Build a serialisable diagnostics report over a recent time window."""
    end = (end_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start = end - timedelta(hours=float(hours))
    summaries = diagnostics_in_range(
        db_path=db_path,
        start_utc=_to_utc_iso(start),
        end_utc=_to_utc_iso(end),
        pod_ids=pod_ids,
    )
    return {
        "generated_at_utc": utc_now_iso(),
        "range_start_utc": _to_utc_iso(start),
        "range_end_utc": _to_utc_iso(end),
        "rows": [asdict(item) for item in summaries],
    }
# Function purpose: Return per-pod diagnostics summaries within an explicit UTC
#   window.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, start_utc, end_utc, pod_ids, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[PodDiagnosticsSummary] when the function completes
#   successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def diagnostics_in_range(
    *,
    db_path: Path | str | None,
    start_utc: str,
    end_utc: str,
    pod_ids: list[str] | None = None,
) -> list[PodDiagnosticsSummary]:
    """Return per-pod diagnostics summaries within an explicit UTC window."""
    resolved_db_path = resolve_db_path(db_path)
    if not resolved_db_path.exists():
        return []

    connection = connect_sqlite(resolved_db_path, readonly=True)
    try:
        sample_rows = _sample_rows(connection, start_utc=start_utc, end_utc=end_utc, pod_ids=pod_ids)
        link_rows = _link_rows(connection, start_utc=start_utc, end_utc=end_utc, pod_ids=pod_ids)
        resend_counts = _resend_counts(connection, start_utc=start_utc, end_utc=end_utc, pod_ids=pod_ids)
    finally:
        connection.close()

    all_pods = sorted(
        {
            *(str(row["pod_id"]) for row in sample_rows),
            *(str(row["pod_id"]) for row in link_rows),
            *([str(item) for item in pod_ids] if pod_ids else []),
        }
    )
    return [
        _build_pod_summary(
            pod_id=pod_id,
            sample_rows=[row for row in sample_rows if str(row["pod_id"]) == pod_id],
            link_rows=[row for row in link_rows if str(row["pod_id"]) == pod_id],
            resend_request_count=resend_counts.get(pod_id, 0),
        )
        for pod_id in all_pods
    ]
# Function purpose: Aggregate one pod's stored communication evidence into a summary
#   row.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as pod_id, sample_rows, link_rows, resend_request_count,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns PodDiagnosticsSummary when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _build_pod_summary(
    *,
    pod_id: str,
    sample_rows: list[dict[str, object]],
    link_rows: list[dict[str, object]],
    resend_request_count: int,
) -> PodDiagnosticsSummary:
    """Aggregate one pod's stored communication evidence into a summary row."""
    ordered_samples = sorted(sample_rows, key=lambda row: (str(row["ts_pc_utc"]), int(row["session_id"]), int(row["seq"])))
    rssi_values = [int(row["rssi"]) for row in ordered_samples if row.get("rssi") is not None]
    if not rssi_values:
        rssi_values = [int(row["last_rssi"]) for row in link_rows if row.get("last_rssi") is not None]

    drift_count = 0
    max_abs_drift = 0.0
    latest_estimated_time: str | None = None
    current_session: int | None = None
    alignment_state = AlignmentState()
    for row in ordered_samples:
        # Timing drift is recomputed from stored rows so diagnostics can be
        # reviewed historically rather than only at the moment of live routing.
        session_id = int(row["session_id"])
        if current_session != session_id:
            current_session = session_id
            alignment_state = AlignmentState()
        if row.get("ts_uptime_s") is None:
            continue
        result = align_sample(
            alignment_state,
            gateway_ts_utc=str(row["ts_pc_utc"]),
            ts_uptime_s=float(row["ts_uptime_s"]),
        )
        if result.anomalous:
            drift_count += 1
        max_abs_drift = max(max_abs_drift, abs(float(result.drift_s)))
        latest_estimated_time = _to_utc_iso(result.estimated_ts_utc)

    link_missing, duplicates, reconnects = _link_deltas(link_rows)
    session_count = len({int(row["session_id"]) for row in ordered_samples})
    return PodDiagnosticsSummary(
        pod_id=pod_id,
        sample_count=len(ordered_samples),
        session_count=session_count,
        avg_rssi=None if not rssi_values else (sum(rssi_values) / float(len(rssi_values))),
        min_rssi=None if not rssi_values else min(rssi_values),
        max_rssi=None if not rssi_values else max(rssi_values),
        missing_samples=link_missing,
        duplicate_count=duplicates,
        reconnect_count=reconnects,
        resend_request_count=int(resend_request_count),
        drift_anomaly_count=drift_count,
        max_abs_drift_s=max_abs_drift,
        latest_estimated_sample_time_utc=latest_estimated_time,
    )
# Function purpose: Implements the sample rows step used by this subsystem.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, start_utc, end_utc, pod_ids, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[dict[str, object]] when the function completes
#   successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _sample_rows(connection, *, start_utc: str, end_utc: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
    query = """
        SELECT ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, rssi, quality_flags
        FROM samples_raw
        WHERE ts_pc_utc >= ?
          AND ts_pc_utc < ?
    """
    parameters: list[object] = [start_utc, end_utc]
    if pod_ids:
        placeholders = ",".join("?" for _ in pod_ids)
        query += f" AND pod_id IN ({placeholders})"
        parameters.extend(str(item) for item in pod_ids)
    query += " ORDER BY pod_id ASC, session_id ASC, ts_pc_utc ASC, seq ASC"
    return [dict(row) for row in connection.execute(query, tuple(parameters)).fetchall()]
# Function purpose: Implements the link rows step used by this subsystem.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, start_utc, end_utc, pod_ids, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[dict[str, object]] when the function completes
#   successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _link_rows(connection, *, start_utc: str, end_utc: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
    try:
        query = """
            SELECT ts_pc_utc, pod_id, last_rssi, total_missing, total_duplicates, reconnect_count
            FROM link_quality
            WHERE ts_pc_utc >= ?
              AND ts_pc_utc < ?
        """
        parameters: list[object] = [start_utc, end_utc]
        if pod_ids:
            placeholders = ",".join("?" for _ in pod_ids)
            query += f" AND pod_id IN ({placeholders})"
            parameters.extend(str(item) for item in pod_ids)
        query += " ORDER BY pod_id ASC, ts_pc_utc ASC"
        return [dict(row) for row in connection.execute(query, tuple(parameters)).fetchall()]
    except Exception:
        return []
# Function purpose: Implements the resend counts step used by this subsystem.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, start_utc, end_utc, pod_ids, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns dict[str, int] when the function completes successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _resend_counts(connection, *, start_utc: str, end_utc: str, pod_ids: list[str] | None) -> dict[str, int]:
    try:
        query = """
            SELECT pod_id, COUNT(*) AS resend_count
            FROM gateway_events
            WHERE ts_pc_utc >= ?
              AND ts_pc_utc < ?
              AND message LIKE 'resend_request%'
        """
        parameters: list[object] = [start_utc, end_utc]
        if pod_ids:
            placeholders = ",".join("?" for _ in pod_ids)
            query += f" AND pod_id IN ({placeholders})"
            parameters.extend(str(item) for item in pod_ids)
        query += " GROUP BY pod_id"
        rows = connection.execute(query, tuple(parameters)).fetchall()
    except Exception:
        return {}
    return {str(row["pod_id"]): int(row["resend_count"]) for row in rows if row["pod_id"] is not None}
# Function purpose: Implements the link deltas step used by this subsystem.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as link_rows, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns tuple[int, int, int] when the function completes successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _link_deltas(link_rows: list[dict[str, object]]) -> tuple[int, int, int]:
    if not link_rows:
        return 0, 0, 0
    ordered = sorted(link_rows, key=lambda row: str(row["ts_pc_utc"]))
    first = ordered[0]
    last = ordered[-1]
    if len(ordered) == 1:
        return (
            int(last.get("total_missing") or 0),
            int(last.get("total_duplicates") or 0),
            int(last.get("reconnect_count") or 0),
        )
    missing = max(int(last.get("total_missing") or 0) - int(first.get("total_missing") or 0), 0)
    duplicates = max(int(last.get("total_duplicates") or 0) - int(first.get("total_duplicates") or 0), 0)
    reconnects = max(int(last.get("reconnect_count") or 0) - int(first.get("reconnect_count") or 0), 0)
    return missing, duplicates, reconnects
# Function purpose: Implements the to UTC iso step used by this subsystem.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def _to_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
