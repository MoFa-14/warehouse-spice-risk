# File overview:
# - Responsibility: Dashboard loaders for stored forecast outputs.
# - Project role: Loads persisted telemetry, forecast, or evaluation data for later
#   dashboard interpretation.
# - Main data or concerns: Telemetry rows, forecast rows, evaluation rows, and path
#   filters.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

"""Dashboard loaders for stored forecast outputs.

Responsibilities:
- Reloads the latest forecasts, historical forecast windows, and evaluation
  history from the forecast archive.
- Normalises storage rows into predictable pandas frames for the dashboard
  service layer.

Project flow:
- stored forecasts/evaluations -> dashboard data-access tables -> prediction
  services -> rendered views and charts

Why this matters:
- The dashboard explanation layer depends on a stable schema regardless of
  whether the underlying archive is SQLite or legacy JSONL.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from app.data_access.sqlite_reader import _connect, sqlite_db_exists


FORECAST_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "scenario",
    "horizon_min",
    "json_forecast",
    "json_p25",
    "json_p75",
    "event_detected",
    "event_type",
    "event_reason",
    "model_version",
    "MAE_T",
    "RMSE_T",
    "MAE_RH",
    "RMSE_RH",
    "large_error",
    "evaluation_notes",
]

EVALUATION_COLUMNS = [
    "ts_forecast_utc",
    "pod_id",
    "scenario",
    "MAE_T",
    "RMSE_T",
    "MAE_RH",
    "RMSE_RH",
    "PERSIST_MAE_T",
    "PERSIST_RMSE_T",
    "PERSIST_MAE_RH",
    "PERSIST_RMSE_RH",
    "large_error",
    "event_detected",
    "evaluation_notes",
]


# Latest-forecast loader
# - Purpose: returns the newest stored forecast rows per pod, already joined to
#   any available evaluation metadata.
# - Project role: primary read path for the live prediction page.
# Function purpose: Return the latest stored forecast rows per pod, joined with
#   evaluations.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, interpreted according to
#   the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def read_latest_forecasts(
    data_root: Path,
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
) -> pd.DataFrame:
    """Return the latest stored forecast rows per pod, joined with evaluations."""
    if sqlite_db_exists(db_path) and _sqlite_has_forecast_tables(db_path):
        return _read_latest_forecasts_sqlite(db_path, pod_id=pod_id)
    return _read_latest_forecasts_jsonl(Path(data_root), pod_id=pod_id)


# Historical forecast-window loader
# - Purpose: returns forecast rows within a requested UTC time range.
# - Project role: historical evidence read path used by the forecast-test card.
# Function purpose: Return forecast rows within a UTC window.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, start_utc, end_utc, pod_id, interpreted
#   according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def read_forecasts_in_window(
    db_path: Path | str | None,
    *,
    start_utc: str,
    end_utc: str,
    pod_id: str | None = None,
) -> pd.DataFrame:
    """Return forecast rows within a UTC window."""
    if not sqlite_db_exists(db_path) or not _sqlite_has_forecast_tables(db_path):
        return pd.DataFrame(columns=FORECAST_COLUMNS)

    query = """
        SELECT f.ts_pc_utc, f.pod_id, f.scenario, f.horizon_min, f.json_forecast, f.json_p25, f.json_p75,
               f.event_detected, f.event_type, f.event_reason, f.model_version,
               e.MAE_T, e.RMSE_T, e.MAE_RH, e.RMSE_RH, e.large_error, e.notes AS evaluation_notes
        FROM forecasts AS f
        LEFT JOIN evaluations AS e
            ON e.pod_id = f.pod_id
           AND e.ts_forecast_utc = f.ts_pc_utc
           AND e.scenario = f.scenario
        WHERE f.ts_pc_utc >= ?
          AND f.ts_pc_utc < ?
    """
    parameters: list[object] = [start_utc, end_utc]
    if pod_id is not None:
        query += " AND f.pod_id = ?"
        parameters.append(str(pod_id))
    query += " ORDER BY f.pod_id ASC, f.ts_pc_utc ASC, f.scenario ASC"

    connection = _connect(db_path)
    try:
        frame = pd.read_sql_query(query, connection, params=parameters)
    finally:
        connection.close()
    return _normalize_forecast_frame(frame)


# Evaluation-history loader
# - Purpose: loads stored evaluation rows for one scenario and optional pod
#   filter.
# - Downstream dependency: feeds the dashboard's model-vs-persistence charts
#   and historical summary views.
# Function purpose: Return stored evaluation history for one scenario across one or
#   more pods.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, scenario, interpreted
#   according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def read_evaluation_history(
    data_root: Path,
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    scenario: str = "baseline",
) -> pd.DataFrame:
    """Return stored evaluation history for one scenario across one or more pods."""
    if sqlite_db_exists(db_path) and _sqlite_has_forecast_tables(db_path):
        return _read_evaluation_history_sqlite(db_path, pod_id=pod_id, scenario=scenario)
    return _read_evaluation_history_jsonl(Path(data_root), pod_id=pod_id, scenario=scenario)


# SQLite latest-forecast query
# - Purpose: finds the newest stored forecast timestamp per pod and returns all
#   scenario rows tied to that timestamp.
# Function purpose: Read the latest per-pod forecast rows directly from SQLite.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, interpreted according to the
#   implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _read_latest_forecasts_sqlite(db_path: Path | str | None, *, pod_id: str | None) -> pd.DataFrame:
    """Read the latest per-pod forecast rows directly from SQLite."""
    query = """
        SELECT f.ts_pc_utc, f.pod_id, f.scenario, f.horizon_min, f.json_forecast, f.json_p25, f.json_p75,
               f.event_detected, f.event_type, f.event_reason, f.model_version,
               e.MAE_T, e.RMSE_T, e.MAE_RH, e.RMSE_RH, e.large_error, e.notes AS evaluation_notes
        FROM forecasts AS f
        INNER JOIN (
            SELECT pod_id, MAX(ts_pc_utc) AS latest_ts
            FROM forecasts
            GROUP BY pod_id
        ) AS latest
            ON latest.pod_id = f.pod_id
           AND latest.latest_ts = f.ts_pc_utc
        LEFT JOIN evaluations AS e
            ON e.pod_id = f.pod_id
           AND e.ts_forecast_utc = f.ts_pc_utc
           AND e.scenario = f.scenario
    """
    parameters: list[object] = []
    if pod_id is not None:
        query += " WHERE f.pod_id = ?"
        parameters.append(str(pod_id))
    query += " ORDER BY f.pod_id ASC, f.scenario ASC"

    connection = _connect(db_path)
    try:
        frame = pd.read_sql_query(query, connection, params=parameters)
    finally:
        connection.close()
    return _normalize_forecast_frame(frame)


# SQLite evaluation-history query
# - Purpose: reads stored evaluation history while tolerating archives that were
#   created before some optional persistence fields existed.
# Function purpose: Read stored evaluation history from SQLite, including
#   persistence fields when present.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, pod_id, scenario, interpreted according to
#   the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _read_evaluation_history_sqlite(
    db_path: Path | str | None,
    *,
    pod_id: str | None,
    scenario: str,
) -> pd.DataFrame:
    """Read stored evaluation history from SQLite, including persistence fields when present."""
    connection = _connect(db_path)
    try:
        columns = _sqlite_columns(connection, "evaluations")
        query = f"""
            SELECT ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH,
                   {_optional_column(columns, "PERSIST_MAE_T")},
                   {_optional_column(columns, "PERSIST_RMSE_T")},
                   {_optional_column(columns, "PERSIST_MAE_RH")},
                   {_optional_column(columns, "PERSIST_RMSE_RH")},
                   large_error, event_detected, notes AS evaluation_notes
            FROM evaluations
            WHERE scenario = ?
        """
        parameters: list[object] = [str(scenario)]
        if pod_id is not None:
            query += " AND pod_id = ?"
            parameters.append(str(pod_id))
        query += " ORDER BY pod_id ASC, ts_forecast_utc ASC"
        frame = pd.read_sql_query(query, connection, params=parameters)
    except sqlite3.OperationalError:
        return pd.DataFrame(columns=EVALUATION_COLUMNS)
    finally:
        connection.close()
    return _normalize_evaluation_frame(frame)


# Function purpose: Fallback loader for older/offline JSONL-based forecast storage.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, interpreted according to the
#   implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _read_latest_forecasts_jsonl(data_root: Path, *, pod_id: str | None) -> pd.DataFrame:
    """Fallback loader for older/offline JSONL-based forecast storage."""
    forecasts_path = Path(data_root) / "ml" / "forecasts.jsonl"
    evaluations_path = Path(data_root) / "ml" / "evaluations.jsonl"
    forecast_rows = _read_jsonl(forecasts_path)
    evaluation_rows = _read_jsonl(evaluations_path)
    if not forecast_rows:
        return pd.DataFrame(columns=FORECAST_COLUMNS)

    if pod_id is not None:
        forecast_rows = [row for row in forecast_rows if str(row.get("pod_id")) == str(pod_id)]
    if not forecast_rows:
        return pd.DataFrame(columns=FORECAST_COLUMNS)

    latest_by_pod: dict[str, str] = {}
    for row in forecast_rows:
        key = str(row["pod_id"])
        latest_by_pod[key] = max(latest_by_pod.get(key, ""), str(row["ts_pc_utc"]))

    evaluation_lookup = {
        (str(row["pod_id"]), str(row["ts_forecast_utc"]), str(row["scenario"])): row
        for row in evaluation_rows
    }

    records: list[dict[str, object]] = []
    for row in forecast_rows:
        key = str(row["pod_id"])
        if str(row["ts_pc_utc"]) != latest_by_pod[key]:
            continue
        evaluation = evaluation_lookup.get((str(row["pod_id"]), str(row["ts_pc_utc"]), str(row["scenario"])), {})
        records.append(
            {
                "ts_pc_utc": row["ts_pc_utc"],
                "pod_id": row["pod_id"],
                "scenario": row["scenario"],
                "horizon_min": row["horizon_min"],
                "json_forecast": json.dumps(row["json_forecast"], separators=(",", ":"), sort_keys=True),
                "json_p25": json.dumps(row["json_p25"], separators=(",", ":")),
                "json_p75": json.dumps(row["json_p75"], separators=(",", ":")),
                "event_detected": row["event_detected"],
                "event_type": row["event_type"],
                "event_reason": row["event_reason"],
                "model_version": row["model_version"],
                "MAE_T": evaluation.get("MAE_T"),
                "RMSE_T": evaluation.get("RMSE_T"),
                "MAE_RH": evaluation.get("MAE_RH"),
                "RMSE_RH": evaluation.get("RMSE_RH"),
                "large_error": evaluation.get("large_error"),
                "evaluation_notes": evaluation.get("notes"),
            }
        )
    return _normalize_forecast_frame(pd.DataFrame(records, columns=FORECAST_COLUMNS))


# Function purpose: Fallback loader for JSONL-based evaluation history.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, scenario, interpreted according to
#   the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _read_evaluation_history_jsonl(data_root: Path, *, pod_id: str | None, scenario: str) -> pd.DataFrame:
    """Fallback loader for JSONL-based evaluation history."""
    evaluations_path = Path(data_root) / "ml" / "evaluations.jsonl"
    evaluation_rows = _read_jsonl(evaluations_path)
    if not evaluation_rows:
        return pd.DataFrame(columns=EVALUATION_COLUMNS)

    records = [
        {
            "ts_forecast_utc": row.get("ts_forecast_utc"),
            "pod_id": row.get("pod_id"),
            "scenario": row.get("scenario"),
            "MAE_T": row.get("MAE_T"),
            "RMSE_T": row.get("RMSE_T"),
            "MAE_RH": row.get("MAE_RH"),
            "RMSE_RH": row.get("RMSE_RH"),
            "PERSIST_MAE_T": row.get("PERSIST_MAE_T"),
            "PERSIST_RMSE_T": row.get("PERSIST_RMSE_T"),
            "PERSIST_MAE_RH": row.get("PERSIST_MAE_RH"),
            "PERSIST_RMSE_RH": row.get("PERSIST_RMSE_RH"),
            "large_error": row.get("large_error"),
            "event_detected": row.get("event_detected"),
            "evaluation_notes": row.get("notes"),
        }
        for row in evaluation_rows
        if str(row.get("scenario") or "") == str(scenario)
        and (pod_id is None or str(row.get("pod_id")) == str(pod_id))
    ]
    return _normalize_evaluation_frame(pd.DataFrame(records, columns=EVALUATION_COLUMNS))


# Forecast-frame normalisation
# - Purpose: converts raw query output into the consistent schema expected by
#   dashboard services.
# Function purpose: Standardise forecast rows into one predictable dashboard-ready
#   schema.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as frame, interpreted according to the implementation
#   below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _normalize_forecast_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardise forecast rows into one predictable dashboard-ready schema."""
    if frame.empty:
        return pd.DataFrame(columns=FORECAST_COLUMNS)
    frame = frame.copy()
    frame["ts_pc_utc"] = pd.to_datetime(frame["ts_pc_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts_pc_utc"]).sort_values(["pod_id", "ts_pc_utc", "scenario"], kind="mergesort")
    frame["pod_id"] = frame["pod_id"].astype("string").fillna("").astype(str)
    frame["scenario"] = frame["scenario"].astype("string").fillna("").astype(str)
    frame["event_type"] = frame["event_type"].fillna("").astype(str)
    frame["event_reason"] = frame["event_reason"].fillna("").astype(str)
    frame["model_version"] = frame["model_version"].fillna("").astype(str)
    frame["evaluation_notes"] = frame["evaluation_notes"].fillna("").astype(str)
    frame["event_detected"] = frame["event_detected"].fillna(0).astype(bool)
    for column in ("horizon_min", "MAE_T", "RMSE_T", "MAE_RH", "RMSE_RH", "large_error"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[FORECAST_COLUMNS].reset_index(drop=True)


# Evaluation-frame normalisation
# - Purpose: converts raw evaluation rows into the stable schema used by
#   dashboard comparison logic.
# Function purpose: Standardise evaluation-history rows into one predictable schema.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as frame, interpreted according to the implementation
#   below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _normalize_evaluation_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardise evaluation-history rows into one predictable schema."""
    if frame.empty:
        return pd.DataFrame(columns=EVALUATION_COLUMNS)
    frame = frame.copy()
    frame["ts_forecast_utc"] = pd.to_datetime(frame["ts_forecast_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["ts_forecast_utc"]).sort_values(["pod_id", "ts_forecast_utc"], kind="mergesort")
    frame["pod_id"] = frame["pod_id"].astype("string").fillna("").astype(str)
    frame["scenario"] = frame["scenario"].astype("string").fillna("").astype(str)
    frame["evaluation_notes"] = frame["evaluation_notes"].fillna("").astype(str)
    frame["event_detected"] = frame["event_detected"].fillna(0).astype(bool)
    for column in (
        "MAE_T",
        "RMSE_T",
        "MAE_RH",
        "RMSE_RH",
        "PERSIST_MAE_T",
        "PERSIST_RMSE_T",
        "PERSIST_MAE_RH",
        "PERSIST_RMSE_RH",
        "large_error",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[EVALUATION_COLUMNS].reset_index(drop=True)


# Function purpose: Check whether the selected SQLite database actually contains
#   forecast tables.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as db_path, interpreted according to the implementation
#   below.
# - Outputs: Returns bool when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _sqlite_has_forecast_tables(db_path: Path | str | None) -> bool:
    """Check whether the selected SQLite database actually contains forecast tables."""
    connection = _connect(db_path)
    try:
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        connection.close()
    return {"forecasts", "evaluations"} <= tables


# Function purpose: Read newline-delimited JSON records from disk.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as path, interpreted according to the implementation
#   below.
# - Outputs: Returns list[dict[str, object]] when the function completes
#   successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _read_jsonl(path: Path) -> list[dict[str, object]]:
    """Read newline-delimited JSON records from disk."""
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


# Function purpose: Return SQLite column names so dashboard loading code can stay
#   backward-compatible.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, table_name, interpreted according to the
#   implementation below.
# - Outputs: Returns set[str] when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _sqlite_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    """Return SQLite column names so dashboard loading code can stay backward-compatible."""
    return {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


# Function purpose: Request a real column when available, or a NULL placeholder when
#   not.
# - Project role: Belongs to the dashboard data-loading layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as columns, name, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Persistence-facing code keeps schema and loading rules
#   centralized so later stages do not duplicate storage assumptions.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.

def _optional_column(columns: set[str], name: str) -> str:
    """Request a real column when available, or a NULL placeholder when not."""
    return name if name in columns else f"NULL AS {name}"
