# File overview:
# - Responsibility: SQLite loaders for dashboard telemetry access.
# - Project role: Loads persisted telemetry, forecast, or evaluation data for later
#   dashboard interpretation.
# - Main data or concerns: Telemetry rows, forecast rows, evaluation rows, and path
#   filters.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

"""SQLite loaders for dashboard telemetry access.

Responsibilities:
- Loads raw telemetry and link-quality history from the live runtime database.
- Normalises those rows into the schema expected by dashboard services.
- Recomputes dew point where needed so the service layer always receives a full
  psychrometric context.

Project flow:
- SQLite storage -> dashboard data-access frames -> service-layer summaries,
  charts, and route context
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


# Database existence check
# - Purpose: confirms that a candidate SQLite path points to a non-empty file
#   before higher-level loading code attempts database access.
# Function purpose: Handles database exists for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, interpreted according to the implementation
#   below.
# - Outputs: Returns bool when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def sqlite_db_exists(db_path: Path | str | None) -> bool:
    if db_path is None:
        return False
    path = Path(db_path)
    return path.is_file() and path.stat().st_size > 0


# Pod discovery
# - Purpose: builds the dashboard pod list from every table that may contain
#   relevant current or historical evidence.
# - Important decision: pods remain discoverable even when they are no longer
#   currently transmitting, because historical forecasts and evaluations still
#   need to stay browsable.
# Function purpose: Discover pods from every dashboard-relevant SQLite table.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, interpreted according to the implementation
#   below.
# - Outputs: Returns list[str] when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def discover_pod_ids_from_sqlite(db_path: Path | str | None) -> list[str]:
    """Discover pods from every dashboard-relevant SQLite table."""
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


# Raw-telemetry loader
# - Purpose: loads raw telemetry rows into the canonical dashboard dataframe
#   schema.
# - Project role: low-level read path used by higher-level time-series and
#   prediction services.
# - Important decision: stays close to the storage schema so later services can
#   choose their own presentation, filtering, or calibration logic.
# Function purpose: Read raw telemetry rows into a normalised dataframe.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, date_from, date_to, interpreted
#   according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def read_raw_samples_sqlite(
    db_path: Path | str | None,
    *,
    pod_id: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> pd.DataFrame:
    """Read raw telemetry rows into a normalised dataframe."""
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


# Link-quality loader
# - Purpose: loads persisted gateway link diagnostics into the canonical
#   dashboard dataframe schema.
# Function purpose: Read persisted link-quality snapshots into a dataframe.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, date_from, date_to, interpreted
#   according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

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


# SQLite connection helper
# - Purpose: opens a dashboard read connection with the row and journal options
#   expected by the rest of this module.
# Function purpose: Opens connect for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, interpreted according to the implementation
#   below.
# - Outputs: Returns sqlite3.Connection when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _connect(db_path: Path | str | None) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    return connection


# Raw-frame normalisation
# - Purpose: coerces query output into the column types expected by dashboard
#   services and recomputes dew point when necessary.
# Function purpose: Coerce database rows into the column types expected by dashboard
#   logic.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as frame, interpreted according to the implementation
#   below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

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
    # Dew point is recomputed here so the dashboard can always display a
    # complete psychrometric context even when raw storage did not persist dew
    # point explicitly.
    frame["dew_point_c"] = frame.apply(lambda row: _dew_point_c(row.get("temp_c"), row.get("rh_pct")), axis=1)
    return frame[RAW_COLUMNS].copy()


# Link-frame normalisation
# - Purpose: coerces link-quality query output into the column types expected by
#   dashboard services.
# Function purpose: Normalizes link frame for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as frame, interpreted according to the implementation
#   below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

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


# Date-range expansion
# - Purpose: converts inclusive date filters into the UTC timestamp bounds used
#   by SQL queries.
# Function purpose: Handles bounds for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as date_from, date_to, interpreted according to the
#   implementation below.
# - Outputs: Returns tuple[str, str] when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _utc_bounds(date_from: date, date_to: date) -> tuple[str, str]:
    start = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
    end = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return _to_utc_iso(start), _to_utc_iso(end)


# UTC serialisation helper
# - Purpose: keeps dashboard time filters aligned with the repository's stable
#   UTC string format.
# Function purpose: Handles UTC iso for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _to_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# Dew-point reconstruction
# - Purpose: derives dew point from temperature and RH so dashboard calculations
#   always operate on the same variable set used elsewhere in the project.
# Function purpose: Handles point c for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as temp_c, rh_pct, interpreted according to the
#   implementation below.
# - Outputs: Returns the value or side effect defined by the implementation.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _dew_point_c(temp_c, rh_pct):
    if pd.isna(temp_c) or pd.isna(rh_pct):
        return float("nan")
    rh = max(1e-6, min(float(rh_pct), 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * float(temp_c) / (b + float(temp_c))) + math.log(rh)
    return (b * gamma) / (a - gamma)


# Pod sort key
# - Purpose: keeps numeric pod identifiers in numeric order while still
#   tolerating non-numeric pod labels.
# Function purpose: Handles identifier sort key for the surrounding project flow.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns tuple[int, int | str] when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _pod_id_sort_key(value: str) -> tuple[int, int | str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)
