"""SQLite readers for live dashboard telemetry access.

This module is the low-level database access layer for the dashboard's
monitoring pages. It reads raw samples and link-quality history, normalises the
data into pandas frames, and computes dew point where necessary so the service
layer can focus on interpretation and visualisation.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd


RAW_COLUMNS = [
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
]

LINK_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "connected",
    "last_rssi",
    "total_received",
    "total_missing",
    "total_duplicates",
    "disconnect_count",
    "reconnect_count",
    "missing_rate",
]

POD_DISCOVERY_TABLES = (
    "samples_raw",
    "link_quality",
    "forecasts",
    "evaluations",
    "case_base",
)


def sqlite_db_exists(db_path: Path | str | None) -> bool:
    if db_path is None:
        return False
    path = Path(db_path)
    return path.is_file() and path.stat().st_size > 0


def discover_pod_ids_from_sqlite(db_path: Path | str | None) -> list[str]:
    """Discover pods from every dashboard-relevant SQLite table.

    The pod list should reflect not only currently active devices but also pods
    with stored history, forecasts, or evaluations. That behaviour is important
    for the dashboard because historical evidence should remain explorable after
    a pod disconnects.
    """
    if not sqlite_db_exists(db_path):
        return []
    connection = _connect(db_path)
    try:
        tables = {
            str(row["name"])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        pod_ids: set[str] = set()
        for table_name in POD_DISCOVERY_TABLES:
            if table_name not in tables:
                continue
            rows = connection.execute(f"SELECT DISTINCT pod_id FROM {table_name} WHERE pod_id IS NOT NULL").fetchall()
            pod_ids.update(str(row["pod_id"]).strip() for row in rows if str(row["pod_id"]).strip())
    finally:
        connection.close()
    return sorted(pod_ids, key=_pod_id_sort_key)


def read_raw_samples_sqlite(
    db_path: Path | str | None,
    *,
    pod_id: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> pd.DataFrame:
    """Read raw telemetry rows into a normalised dataframe.

    This function stays close to the storage schema on purpose. Higher-level
    services such as ``timeseries_service`` and ``pod_service`` decide how to
    calibrate, filter, and present the data after it has been loaded.
    """
    if not sqlite_db_exists(db_path):
        return pd.DataFrame(columns=RAW_COLUMNS)

    query = """
        SELECT ts_pc_utc, pod_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags
        FROM samples_raw
    """
    parameters: list[object] = []
    where_clauses: list[str] = []
    if pod_id is not None:
        where_clauses.append("pod_id = ?")
        parameters.append(str(pod_id))
    if date_from is not None:
        where_clauses.append("ts_pc_utc >= ?")
        parameters.append(_utc_bounds(date_from, date_to or date_from)[0])
    if date_to is not None:
        where_clauses.append("ts_pc_utc < ?")
        parameters.append(_utc_bounds(date_from or date_to, date_to)[1])
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY pod_id ASC, ts_pc_utc ASC, session_id ASC, seq ASC"

    connection = _connect(db_path)
    try:
        frame = pd.read_sql_query(query, connection, params=parameters)
    finally:
        connection.close()
    return _normalize_raw_frame(frame)


def read_link_quality_sqlite(
    db_path: Path | str | None,
    *,
    pod_id: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> pd.DataFrame:
    """Read persisted link-quality snapshots into a dataframe."""
    if not sqlite_db_exists(db_path):
        return pd.DataFrame(columns=LINK_COLUMNS)

    query = """
        SELECT ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
               total_duplicates, disconnect_count, reconnect_count, missing_rate
        FROM link_quality
    """
    parameters: list[object] = []
    where_clauses: list[str] = []
    if pod_id is not None:
        where_clauses.append("pod_id = ?")
        parameters.append(str(pod_id))
    if date_from is not None:
        where_clauses.append("ts_pc_utc >= ?")
        parameters.append(_utc_bounds(date_from, date_to or date_from)[0])
    if date_to is not None:
        where_clauses.append("ts_pc_utc < ?")
        parameters.append(_utc_bounds(date_from or date_to, date_to)[1])
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY pod_id ASC, ts_pc_utc ASC"

    connection = _connect(db_path)
    try:
        frame = pd.read_sql_query(query, connection, params=parameters)
    finally:
        connection.close()
    return _normalize_link_frame(frame)


def _connect(db_path: Path | str | None) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    return connection


def _normalize_raw_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce database rows into the column types expected by dashboard logic."""
    if frame.empty:
        return pd.DataFrame(columns=RAW_COLUMNS)
    frame = frame.copy()
    frame["ts_pc_utc"] = pd.to_datetime(frame["ts_pc_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts_pc_utc"]).sort_values(["ts_pc_utc", "seq"], kind="mergesort").reset_index(drop=True)
    frame["pod_id"] = frame["pod_id"].astype("string").fillna("").astype(str)
    for column in ("seq", "ts_uptime_s", "temp_c", "rh_pct", "flags", "rssi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["quality_flags"] = frame["quality_flags"].fillna("").astype(str)
    # Dew point is recomputed here so even raw telemetry pages can show a
    # complete psychrometric context without requiring a dedicated stored column.
    frame["dew_point_c"] = frame.apply(lambda row: _dew_point_c(row.get("temp_c"), row.get("rh_pct")), axis=1)
    return frame[RAW_COLUMNS].copy()


def _normalize_link_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=LINK_COLUMNS)
    frame = frame.copy()
    frame["ts_pc_utc"] = pd.to_datetime(frame["ts_pc_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts_pc_utc"]).sort_values(["ts_pc_utc", "pod_id"], kind="mergesort").reset_index(drop=True)
    frame["pod_id"] = frame["pod_id"].astype("string").fillna("").astype(str)
    for column in LINK_COLUMNS:
        if column in {"ts_pc_utc", "pod_id"}:
            continue
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[LINK_COLUMNS].copy()


def _utc_bounds(date_from: date, date_to: date) -> tuple[str, str]:
    start = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
    end = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return _to_utc_iso(start), _to_utc_iso(end)


def _to_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _dew_point_c(temp_c, rh_pct):
    if pd.isna(temp_c) or pd.isna(rh_pct):
        return float("nan")
    rh = max(1e-6, min(float(rh_pct), 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * float(temp_c) / (b + float(temp_c))) + math.log(rh)
    return (b * gamma) / (a - gamma)


def _pod_id_sort_key(value: str) -> tuple[int, int | str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)
