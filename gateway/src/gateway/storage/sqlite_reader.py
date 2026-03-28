"""Read-only query helpers for the telemetry SQLite database."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from gateway.storage.sqlite_db import connect_sqlite


def latest_sample(*, pod_id: str, db_path: Path | str | None = None) -> dict[str, Any] | None:
    """Return the latest sample row for one pod, or None if the pod has no data."""
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


def samples_in_range(
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    ts_from_utc: str | None = None,
    ts_to_utc: str | None = None,
) -> list[dict[str, Any]]:
    """Return raw sample rows ordered by pod and time within an optional UTC window."""
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


def link_quality_in_range(
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    ts_from_utc: str | None = None,
    ts_to_utc: str | None = None,
) -> list[dict[str, Any]]:
    """Return link-quality rows ordered by pod and time within an optional UTC window."""
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


def utc_bounds_for_dates(date_from: date, date_to: date) -> tuple[str, str]:
    """Convert an inclusive UTC date range into an ISO8601 half-open interval."""
    start = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
    end = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return _utc_iso(start), _utc_iso(end)
def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
