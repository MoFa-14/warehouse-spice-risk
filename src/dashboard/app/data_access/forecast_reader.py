"""Readers for stored forecasting outputs used by the dashboard.

This file is the dashboard-facing access layer for the forecasting archive.
Its job is not to *create* forecasts, but to recover them from storage in a
form that the dashboard can explain clearly:

- latest forecast per pod for the live prediction views
- forecast windows within a time range for historical inspection
- evaluation history for comparison against persistence

In viva terms, this is the point where stored research evidence is turned back
into analysis-ready tables.
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


def read_latest_forecasts(
    data_root: Path,
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
) -> pd.DataFrame:
    """Return the latest stored forecast rows per pod, joined with evaluations.

    This is what the prediction page uses when it shows the current live view of
    each pod's forecast.
    """
    if sqlite_db_exists(db_path) and _sqlite_has_forecast_tables(db_path):
        return _read_latest_forecasts_sqlite(db_path, pod_id=pod_id)
    return _read_latest_forecasts_jsonl(Path(data_root), pod_id=pod_id)


def read_forecasts_in_window(
    db_path: Path | str | None,
    *,
    start_utc: str,
    end_utc: str,
    pod_id: str | None = None,
) -> pd.DataFrame:
    """Return forecast rows within a UTC window.

    The historical forecast-test card relies on this function because it needs
    completed forecasts from a chosen past session rather than only the newest
    live forecast.
    """
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


def read_evaluation_history(
    data_root: Path,
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
    scenario: str = "baseline",
) -> pd.DataFrame:
    """Return stored evaluation history for one scenario across one or more pods.

    The returned frame feeds the dashboard chart that compares model RMSE
    against the persistence baseline across completed forecast attempts.
    """
    if sqlite_db_exists(db_path) and _sqlite_has_forecast_tables(db_path):
        return _read_evaluation_history_sqlite(db_path, pod_id=pod_id, scenario=scenario)
    return _read_evaluation_history_jsonl(Path(data_root), pod_id=pod_id, scenario=scenario)


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


def _read_latest_forecasts_jsonl(data_root: Path, *, pod_id: str | None) -> pd.DataFrame:
    """Fallback reader for older/offline JSONL-based forecast storage."""
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


def _read_evaluation_history_jsonl(data_root: Path, *, pod_id: str | None, scenario: str) -> pd.DataFrame:
    """Fallback reader for JSONL-based evaluation history."""
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


def _sqlite_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    """Return column names for a SQLite table so readers can stay backward-compatible."""
    return {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _optional_column(columns: set[str], name: str) -> str:
    """Request a real column when available, or a NULL placeholder when not."""
    return name if name in columns else f"NULL AS {name}"
