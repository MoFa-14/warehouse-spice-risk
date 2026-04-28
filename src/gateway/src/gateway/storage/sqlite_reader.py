# File overview:
# - Responsibility: Read-only query helpers for the telemetry SQLite database.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Read-only query helpers for the telemetry SQLite database.

These functions give downstream tools, scripts, and tests a stable way to read
the live gateway database without re-implementing SQL in many places. The
dashboard has its own higher-level readers, but those readers ultimately depend
on the same storage concepts exposed here.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from gateway.storage.sqlite_db import connect_sqlite
# Function purpose: Return the most recent raw telemetry row for one pod.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as pod_id, db_path, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns dict[str, Any] | None when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def latest_sample(*, pod_id: str, db_path: Path | str | None = None) -> dict[str, Any] | None:
    """Return the most recent raw telemetry row for one pod."""
    connection = connect_sqlite(db_path, readonly=True)
    try:
        row = connection.execute(
            """
            SELECT ts_pc_utc, pod_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
            FROM samples_raw
            WHERE pod_id = ?
            ORDER BY ts_pc_utc DESC, session_id DESC, seq DESC
            LIMIT 1
            """,
            (str(pod_id),),
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        connection.close()
# Function purpose: Return raw sample rows inside an optional UTC interval.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, ts_from_utc, ts_to_utc, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[dict[str, Any]] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def samples_in_range(
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    ts_from_utc: str | None = None,
    ts_to_utc: str | None = None,
) -> list[dict[str, Any]]:
    """Return raw sample rows inside an optional UTC interval.

    This reader intentionally stays close to the storage schema. It is useful
    for export, verification, and any analysis path that should inspect what
    the gateway actually persisted rather than a later processed view.
    """
    connection = connect_sqlite(db_path, readonly=True)
    try:
        where_clauses: list[str] = []
        parameters: list[Any] = []
        if pod_id is not None:
            where_clauses.append("pod_id = ?")
            parameters.append(str(pod_id))
        if ts_from_utc is not None:
            where_clauses.append("ts_pc_utc >= ?")
            parameters.append(ts_from_utc)
        if ts_to_utc is not None:
            where_clauses.append("ts_pc_utc < ?")
            parameters.append(ts_to_utc)

        query = """
            SELECT ts_pc_utc, pod_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
            FROM samples_raw
        """
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        query += " ORDER BY pod_id ASC, ts_pc_utc ASC, session_id ASC, seq ASC"

        return [dict(row) for row in connection.execute(query, tuple(parameters)).fetchall()]
    finally:
        connection.close()
# Function purpose: Return stored link-quality snapshots for one pod or the whole
#   system.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, ts_from_utc, ts_to_utc, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[dict[str, Any]] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def link_quality_in_range(
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    ts_from_utc: str | None = None,
    ts_to_utc: str | None = None,
) -> list[dict[str, Any]]:
    """Return stored link-quality snapshots for one pod or the whole system."""
    connection = connect_sqlite(db_path, readonly=True)
    try:
        where_clauses: list[str] = []
        parameters: list[Any] = []
        if pod_id is not None:
            where_clauses.append("pod_id = ?")
            parameters.append(str(pod_id))
        if ts_from_utc is not None:
            where_clauses.append("ts_pc_utc >= ?")
            parameters.append(ts_from_utc)
        if ts_to_utc is not None:
            where_clauses.append("ts_pc_utc < ?")
            parameters.append(ts_to_utc)

        query = """
            SELECT ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                   total_duplicates, disconnect_count, reconnect_count, missing_rate
            FROM link_quality
        """
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        query += " ORDER BY pod_id ASC, ts_pc_utc ASC"

        return [dict(row) for row in connection.execute(query, tuple(parameters)).fetchall()]
    finally:
        connection.close()
# Function purpose: Convert inclusive day filters into the half-open interval used
#   in SQL.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as date_from, date_to, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns tuple[str, str] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def utc_bounds_for_dates(date_from: date, date_to: date) -> tuple[str, str]:
    """Convert inclusive day filters into the half-open interval used in SQL."""
    start = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
    end = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return _utc_iso(start), _utc_iso(end)
# Function purpose: Implements the UTC iso step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
