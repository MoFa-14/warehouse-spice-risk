"""Readers for stored forecasting outputs used by the dashboard."""

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


def read_latest_forecasts(
    data_root: Path,
    *,
    db_path: Path | str | None = None,
    pod_id: str | None = None,
) -> pd.DataFrame:
    """Return the latest stored forecast rows per pod, joined with any evaluations."""
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
    """Return forecast rows within a UTC window when SQLite forecast tables exist."""
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


def _read_latest_forecasts_sqlite(db_path: Path | str | None, *, pod_id: str | None) -> pd.DataFrame:
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


def _read_latest_forecasts_jsonl(data_root: Path, *, pod_id: str | None) -> pd.DataFrame:
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


def _normalize_forecast_frame(frame: pd.DataFrame) -> pd.DataFrame:
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


def _sqlite_has_forecast_tables(db_path: Path | str | None) -> bool:
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
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows
