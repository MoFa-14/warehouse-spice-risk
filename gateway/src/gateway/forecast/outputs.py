"""Persist forecasts and evaluations in SQLite or JSONL form."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from gateway.forecast import _ensure_forecasting_package
from gateway.storage.paths import build_storage_paths
from gateway.storage.sqlite_db import connect_sqlite, resolve_db_path

_ensure_forecasting_package()

from forecasting.models import EvaluationMetrics, ForecastBundle, ForecastTrajectory


FORECASTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS forecasts (
        ts_pc_utc TEXT NOT NULL,
        pod_id TEXT NOT NULL,
        scenario TEXT NOT NULL,
        horizon_min INTEGER NOT NULL,
        json_forecast TEXT NOT NULL,
        json_p25 TEXT NOT NULL,
        json_p75 TEXT NOT NULL,
        event_detected INTEGER NOT NULL,
        event_type TEXT,
        event_reason TEXT,
        model_version TEXT NOT NULL,
        PRIMARY KEY (pod_id, ts_pc_utc, scenario)
    )
"""

EVALUATIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS evaluations (
        ts_forecast_utc TEXT NOT NULL,
        pod_id TEXT NOT NULL,
        scenario TEXT NOT NULL,
        MAE_T REAL NOT NULL,
        RMSE_T REAL NOT NULL,
        MAE_RH REAL NOT NULL,
        RMSE_RH REAL NOT NULL,
        event_detected INTEGER NOT NULL,
        large_error INTEGER NOT NULL,
        notes TEXT,
        PRIMARY KEY (pod_id, ts_forecast_utc, scenario)
    )
"""


class ForecastOutputs:
    """Write forecasts/evaluations to SQLite or JSONL with stable payloads."""

    def __init__(
        self,
        *,
        storage_backend: str,
        db_path=None,
        data_root=None,
    ) -> None:
        self.storage_backend = storage_backend.strip().lower()
        self.db_path = resolve_db_path(db_path)
        self.data_root = build_storage_paths(data_root).root
        self.forecasts_jsonl = self.data_root / "ml" / "forecasts.jsonl"
        self.evaluations_jsonl = self.data_root / "ml" / "evaluations.jsonl"

    def ensure_storage(self) -> None:
        if self.storage_backend == "sqlite":
            connection = connect_sqlite(self.db_path)
            try:
                connection.execute(FORECASTS_TABLE_SQL)
                connection.execute(EVALUATIONS_TABLE_SQL)
                connection.commit()
            finally:
                connection.close()
            return

        self.forecasts_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self.forecasts_jsonl.touch(exist_ok=True)
        self.evaluations_jsonl.touch(exist_ok=True)

    def save_bundle(self, bundle: ForecastBundle) -> None:
        self.ensure_storage()
        scenarios = [bundle.baseline]
        if bundle.event_persist is not None:
            scenarios.append(bundle.event_persist)
        for scenario in scenarios:
            payload = _scenario_payload(bundle, scenario)
            if self.storage_backend == "sqlite":
                self._save_bundle_sqlite(bundle=bundle, scenario=scenario, payload=payload)
            else:
                self._append_jsonl(
                    path=self.forecasts_jsonl,
                    key_fields=("pod_id", "ts_pc_utc", "scenario"),
                    payload=payload,
                )

    def pending_evaluations(self, *, cutoff_utc: str, pod_ids: list[str] | None = None) -> list[dict[str, object]]:
        self.ensure_storage()
        if self.storage_backend == "sqlite":
            return self._pending_sqlite(cutoff=cutoff_utc, pod_ids=pod_ids)
        return self._pending_jsonl(cutoff=cutoff_utc, pod_ids=pod_ids)

    def save_evaluation(self, evaluation: EvaluationMetrics) -> None:
        self.ensure_storage()
        payload = {
            "ts_forecast_utc": evaluation.ts_forecast_utc,
            "pod_id": evaluation.pod_id,
            "scenario": evaluation.scenario,
            "MAE_T": evaluation.mae_temp_c,
            "RMSE_T": evaluation.rmse_temp_c,
            "MAE_RH": evaluation.mae_rh_pct,
            "RMSE_RH": evaluation.rmse_rh_pct,
            "event_detected": evaluation.event_detected,
            "large_error": evaluation.large_error,
            "notes": evaluation.notes,
        }
        if self.storage_backend == "sqlite":
            connection = connect_sqlite(self.db_path)
            try:
                connection.execute(EVALUATIONS_TABLE_SQL)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO evaluations (
                        ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH,
                        event_detected, large_error, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["ts_forecast_utc"],
                        payload["pod_id"],
                        payload["scenario"],
                        payload["MAE_T"],
                        payload["RMSE_T"],
                        payload["MAE_RH"],
                        payload["RMSE_RH"],
                        1 if payload["event_detected"] else 0,
                        1 if payload["large_error"] else 0,
                        payload["notes"],
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            return
        self._append_jsonl(
            path=self.evaluations_jsonl,
            key_fields=("pod_id", "ts_forecast_utc", "scenario"),
            payload=payload,
        )

    def trajectory_from_record(self, record: dict[str, object]) -> ForecastTrajectory:
        forecast_json = _coerce_json(record["json_forecast"])
        p25_json = _coerce_json(record["json_p25"])
        p75_json = _coerce_json(record["json_p75"])
        return ForecastTrajectory(
            scenario=str(record["scenario"]),
            temp_forecast_c=[float(value) for value in forecast_json["temp_forecast_c"]],
            rh_forecast_pct=[float(value) for value in forecast_json["rh_forecast_pct"]],
            dew_point_forecast_c=[float(value) for value in forecast_json.get("dew_point_forecast_c", [])],
            temp_p25_c=[float(value) for value in p25_json["temp_c"]],
            temp_p75_c=[float(value) for value in p75_json["temp_c"]],
            rh_p25_pct=[float(value) for value in p25_json["rh_pct"]],
            rh_p75_pct=[float(value) for value in p75_json["rh_pct"]],
            source=str(forecast_json.get("source") or "unknown"),
            neighbor_count=int(forecast_json.get("neighbor_count") or 0),
            case_count=int(forecast_json.get("case_count") or 0),
            notes=str(forecast_json.get("notes") or ""),
        )

    def feature_vector_from_record(self, record: dict[str, object]) -> dict[str, float]:
        forecast_json = _coerce_json(record["json_forecast"])
        raw_features = forecast_json.get("feature_vector") or {}
        return {str(key): float(value) for key, value in raw_features.items()}

    def forecast_missing_rate(self, record: dict[str, object]) -> float:
        forecast_json = _coerce_json(record["json_forecast"])
        return float(forecast_json.get("missing_rate") or 0.0)

    def _save_bundle_sqlite(self, *, bundle: ForecastBundle, scenario: ForecastTrajectory, payload: dict[str, object]) -> None:
        connection = connect_sqlite(self.db_path)
        try:
            connection.execute(FORECASTS_TABLE_SQL)
            connection.execute(
                """
                INSERT OR REPLACE INTO forecasts (
                    ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                    event_detected, event_type, event_reason, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bundle.ts_pc_utc,
                    bundle.pod_id,
                    scenario.scenario,
                    len(scenario.temp_forecast_c),
                    json.dumps(payload["json_forecast"], separators=(",", ":"), sort_keys=True),
                    json.dumps(payload["json_p25"], separators=(",", ":")),
                    json.dumps(payload["json_p75"], separators=(",", ":")),
                    1 if bundle.event.event_detected else 0,
                    bundle.event.event_type,
                    bundle.event.event_reason,
                    bundle.model_version,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def _pending_sqlite(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            query = """
                SELECT f.ts_pc_utc, f.pod_id, f.scenario, f.horizon_min, f.json_forecast, f.json_p25, f.json_p75,
                       f.event_detected, f.event_type, f.event_reason, f.model_version
                FROM forecasts AS f
                LEFT JOIN evaluations AS e
                    ON e.pod_id = f.pod_id
                   AND e.ts_forecast_utc = f.ts_pc_utc
                   AND e.scenario = f.scenario
                WHERE e.ts_forecast_utc IS NULL
                  AND f.ts_pc_utc <= ?
            """
            parameters: list[object] = [cutoff]
            if pod_ids:
                placeholders = ",".join("?" for _ in pod_ids)
                query += f" AND f.pod_id IN ({placeholders})"
                parameters.extend(pod_ids)
            query += " ORDER BY f.ts_pc_utc ASC, f.pod_id ASC, f.scenario ASC"
            rows = connection.execute(query, tuple(parameters)).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError:
            return []
        finally:
            connection.close()

    def _pending_jsonl(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        forecasts = self._read_jsonl(self.forecasts_jsonl)
        evaluations = self._read_jsonl(self.evaluations_jsonl)
        evaluated_keys = {
            (str(item["pod_id"]), str(item["ts_forecast_utc"]), str(item["scenario"]))
            for item in evaluations
        }
        pending: list[dict[str, object]] = []
        pod_filter = set(pod_ids or [])
        for item in forecasts:
            key = (str(item["pod_id"]), str(item["ts_pc_utc"]), str(item["scenario"]))
            if key in evaluated_keys:
                continue
            if pod_filter and str(item["pod_id"]) not in pod_filter:
                continue
            if str(item["ts_pc_utc"]) > cutoff:
                continue
            pending.append(item)
        pending.sort(key=lambda item: (str(item["ts_pc_utc"]), str(item["pod_id"]), str(item["scenario"])))
        return pending

    def _append_jsonl(self, *, path: Path, key_fields: tuple[str, ...], payload: dict[str, object]) -> None:
        existing = {
            tuple(str(item[field]) for field in key_fields)
            for item in self._read_jsonl(path)
        }
        candidate_key = tuple(str(payload[field]) for field in key_fields)
        if candidate_key in existing:
            return
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True))
            handle.write("\n")

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, object]]:
        if not path.exists():
            return []
        items: list[dict[str, object]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    items.append(json.loads(line))
        return items


def _scenario_payload(bundle: ForecastBundle, scenario: ForecastTrajectory) -> dict[str, object]:
    return {
        "ts_pc_utc": bundle.ts_pc_utc,
        "pod_id": bundle.pod_id,
        "scenario": scenario.scenario,
        "horizon_min": len(scenario.temp_forecast_c),
        "event_detected": bundle.event.event_detected,
        "event_type": bundle.event.event_type,
        "event_reason": bundle.event.event_reason,
        "model_version": bundle.model_version,
        "json_forecast": {
            "temp_forecast_c": scenario.temp_forecast_c,
            "rh_forecast_pct": scenario.rh_forecast_pct,
            "dew_point_forecast_c": scenario.dew_point_forecast_c,
            "feature_vector": bundle.feature_vector.values,
            "missing_rate": bundle.missing_rate,
            "source": scenario.source,
            "neighbor_count": scenario.neighbor_count,
            "case_count": scenario.case_count,
            "notes": scenario.notes,
        },
        "json_p25": {
            "temp_c": scenario.temp_p25_c,
            "rh_pct": scenario.rh_p25_pct,
        },
        "json_p75": {
            "temp_c": scenario.temp_p75_c,
            "rh_pct": scenario.rh_p75_pct,
        },
    }


def _coerce_json(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return json.loads(str(value))
