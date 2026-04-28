# File overview:
# - Responsibility: Forecast and evaluation persistence for the gateway pipeline.
# - Project role: Connects stored telemetry to forecasting, persistence, evaluation,
#   and calibration behavior.
# - Main data or concerns: History windows, forecast bundles, evaluation rows, and
#   calibration metadata.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

"""Forecast and evaluation persistence for the gateway pipeline.

Responsibilities:
- Stores forecast scenarios and later evaluation metrics in SQLite or JSONL.
- Preserves the metadata needed for dashboard explanation, historical review,
  and recent-bias calibration.

Project flow:
- generated forecast bundle -> stored scenario rows -> later evaluation rows ->
  dashboard read path + calibration read path

Why this matters:
- Forecasts become useful to the wider project only once they are stored in a
  stable schema that later services can reload consistently.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from statistics import median

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
        PERSIST_MAE_T REAL,
        PERSIST_RMSE_T REAL,
        PERSIST_MAE_RH REAL,
        PERSIST_RMSE_RH REAL,
        BIAS_T REAL NOT NULL DEFAULT 0,
        BIAS_RH REAL NOT NULL DEFAULT 0,
        event_detected INTEGER NOT NULL,
        large_error INTEGER NOT NULL,
        notes TEXT,
        PRIMARY KEY (pod_id, ts_forecast_utc, scenario)
    )
"""


# Class purpose: Median signed forecast bias over recent stored evaluations.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

@dataclass(frozen=True)
class RecentBias:
    """Median signed forecast bias over recent stored evaluations.

    This is intentionally simple and explainable: it is not a retrained model,
    just a compact summary of whether recent forecasts have tended to sit above
    or below reality.
    """

    temp_c: float
    rh_pct: float
    sample_count: int


# Forecast archive adapter
# - Purpose: writes and reloads forecast/evaluation artefacts using a stable
#   storage contract.
# - Project role: persistence boundary between runtime forecasting and later
#   dashboard or calibration reads.
# - Inputs: storage backend selection plus output locations.
# - Outputs: durable forecast rows, evaluation rows, recent-bias summaries, and
#   reconstructed in-memory trajectory objects.
# Class purpose: Write forecasts/evaluations to SQLite or JSONL with stable
#   payloads.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

class ForecastOutputs:
    """Write forecasts/evaluations to SQLite or JSONL with stable payloads."""

    # Method purpose: Handles init for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as storage_backend, db_path, data_root,
    #   interpreted according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

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

    # Storage initialisation
    # - Purpose: ensures the forecast/evaluation archive exists before reads or
    #   writes proceed.
    # Method purpose: Create forecast/evaluation storage if it does not already
    #   exist.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def ensure_storage(self) -> None:
        """Create forecast/evaluation storage if it does not already exist."""
        if self.storage_backend == "sqlite":
            connection = connect_sqlite(self.db_path)
            try:
                connection.execute(FORECASTS_TABLE_SQL)
                connection.execute(EVALUATIONS_TABLE_SQL)
                _ensure_evaluation_columns(connection)
                connection.commit()
            finally:
                connection.close()
            return

        self.forecasts_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self.forecasts_jsonl.touch(exist_ok=True)
        self.evaluations_jsonl.touch(exist_ok=True)

    # Forecast bundle persistence
    # - Purpose: stores the generated forecast bundle in a scenario-per-row
    #   format.
    # - Project role: write path immediately after forecast generation.
    # - Important decision: one bundle may hold baseline and event-persist
    #   scenarios, but storage uses one row per scenario for simpler later
    #   querying.
    # Method purpose: Persist one forecast bundle.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as bundle, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def save_bundle(self, bundle: ForecastBundle) -> None:
        """Persist one forecast bundle."""
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

    # Due-evaluation lookup
    # - Purpose: returns forecast rows whose horizon has elapsed but which still
    #   lack evaluation records.
    # Method purpose: Return forecasts that are old enough to evaluate and not
    #   yet scored.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff_utc, pod_ids, interpreted according to
    #   the implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def pending_evaluations(self, *, cutoff_utc: str, pod_ids: list[str] | None = None) -> list[dict[str, object]]:
        """Return forecasts that are old enough to evaluate and not yet scored."""
        self.ensure_storage()
        if self.storage_backend == "sqlite":
            return self._pending_sqlite(cutoff=cutoff_utc, pod_ids=pod_ids)
        return self._pending_jsonl(cutoff=cutoff_utc, pod_ids=pod_ids)

    # Persistence-backfill lookup
    # - Purpose: finds older evaluation rows that still need model-vs-
    #   persistence comparison fields.
    # Method purpose: Return older evaluations that still need persistence
    #   comparison fields.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff_utc, pod_ids, interpreted according to
    #   the implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def pending_persistence_backfill(self, *, cutoff_utc: str, pod_ids: list[str] | None = None) -> list[dict[str, object]]:
        """Return older evaluations that still need persistence comparison fields."""
        self.ensure_storage()
        if self.storage_backend == "sqlite":
            return self._pending_persistence_backfill_sqlite(cutoff=cutoff_utc, pod_ids=pod_ids)
        return self._pending_persistence_backfill_jsonl(cutoff=cutoff_utc, pod_ids=pod_ids)

    # Evaluation persistence
    # - Purpose: stores one completed evaluation row.
    # - Downstream dependency: dashboard history, recent-bias calibration, and
    #   later historical review all depend on these saved metrics.
    # Method purpose: Persist one completed evaluation record.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as evaluation, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def save_evaluation(self, evaluation: EvaluationMetrics) -> None:
        """Persist one completed evaluation record."""
        self.ensure_storage()
        payload = {
            "ts_forecast_utc": evaluation.ts_forecast_utc,
            "pod_id": evaluation.pod_id,
            "scenario": evaluation.scenario,
            "MAE_T": evaluation.mae_temp_c,
            "RMSE_T": evaluation.rmse_temp_c,
            "MAE_RH": evaluation.mae_rh_pct,
            "RMSE_RH": evaluation.rmse_rh_pct,
            "PERSIST_MAE_T": evaluation.persistence_mae_temp_c,
            "PERSIST_RMSE_T": evaluation.persistence_rmse_temp_c,
            "PERSIST_MAE_RH": evaluation.persistence_mae_rh_pct,
            "PERSIST_RMSE_RH": evaluation.persistence_rmse_rh_pct,
            "BIAS_T": evaluation.bias_temp_c,
            "BIAS_RH": evaluation.bias_rh_pct,
            "event_detected": evaluation.event_detected,
            "large_error": evaluation.large_error,
            "notes": evaluation.notes,
        }
        if self.storage_backend == "sqlite":
            connection = connect_sqlite(self.db_path)
            try:
                connection.execute(EVALUATIONS_TABLE_SQL)
                _ensure_evaluation_columns(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO evaluations (
                        ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH,
                        PERSIST_MAE_T, PERSIST_RMSE_T, PERSIST_MAE_RH, PERSIST_RMSE_RH,
                        BIAS_T, BIAS_RH, event_detected, large_error, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["ts_forecast_utc"],
                        payload["pod_id"],
                        payload["scenario"],
                        payload["MAE_T"],
                        payload["RMSE_T"],
                        payload["MAE_RH"],
                        payload["RMSE_RH"],
                        payload["PERSIST_MAE_T"],
                        payload["PERSIST_RMSE_T"],
                        payload["PERSIST_MAE_RH"],
                        payload["PERSIST_RMSE_RH"],
                        payload["BIAS_T"],
                        payload["BIAS_RH"],
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
            replace_existing=True,
        )

    # Recent-bias read path
    # - Purpose: derives a compact signed-bias summary from recent trustworthy
    #   evaluation rows.
    # - Project role: input to the runner's lightweight calibration step.
    # Method purpose: Estimate recent signed bias for lightweight
    #   auto-calibration.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as pod_id, scenario, limit, interpreted according
    #   to the implementation below.
    # - Outputs: Returns RecentBias | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def recent_bias(self, *, pod_id: str, scenario: str = "baseline", limit: int = 12) -> RecentBias | None:
        """Estimate recent signed bias for lightweight auto-calibration."""
        self.ensure_storage()
        if limit <= 0:
            return None
        if self.storage_backend == "sqlite":
            return self._recent_bias_sqlite(pod_id=pod_id, scenario=scenario, limit=limit)
        return self._recent_bias_jsonl(pod_id=pod_id, scenario=scenario, limit=limit)

    # Trajectory reconstruction
    # - Purpose: converts one stored forecast row back into the in-memory
    #   trajectory type used by evaluation and dashboard logic.
    # Method purpose: Rebuild an in-memory trajectory from the stored forecast
    #   payload.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as record, interpreted according to the
    #   implementation below.
    # - Outputs: Returns ForecastTrajectory when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def trajectory_from_record(self, record: dict[str, object]) -> ForecastTrajectory:
        """Rebuild an in-memory trajectory from the stored forecast payload."""
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

    # Feature-vector recovery
    # - Purpose: reloads the feature vector that anchored the original forecast.
    # Method purpose: Recover the stored feature vector that anchored the
    #   forecast.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as record, interpreted according to the
    #   implementation below.
    # - Outputs: Returns dict[str, float] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def feature_vector_from_record(self, record: dict[str, object]) -> dict[str, float]:
        """Recover the stored feature vector that anchored the forecast."""
        forecast_json = _coerce_json(record["json_forecast"])
        raw_features = forecast_json.get("feature_vector") or {}
        return {str(key): float(value) for key, value in raw_features.items()}

    # Missing-rate recovery
    # - Purpose: extracts the forecast-input missing-rate saved with a scenario
    #   row.
    # Method purpose: Read the missing-rate metadata saved with the forecast
    #   row.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as record, interpreted according to the
    #   implementation below.
    # - Outputs: Returns float when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def forecast_missing_rate(self, record: dict[str, object]) -> float:
        """Read the missing-rate metadata saved with the forecast row."""
        forecast_json = _coerce_json(record["json_forecast"])
        return float(forecast_json.get("missing_rate") or 0.0)

    # SQLite scenario write
    # - Purpose: writes one scenario row into the integrated runtime database.
    # Method purpose: Write one scenario row into SQLite.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as bundle, scenario, payload, interpreted
    #   according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _save_bundle_sqlite(self, *, bundle: ForecastBundle, scenario: ForecastTrajectory, payload: dict[str, object]) -> None:
        """Write one scenario row into SQLite."""
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

    # SQLite due-evaluation query
    # - Purpose: returns unevaluated forecast rows up to the supplied cutoff.
    # Method purpose: Query SQLite for forecasts that still need evaluation.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _pending_sqlite(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        """Query SQLite for forecasts that still need evaluation."""
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

    # Method purpose: Perform the same pending-evaluation lookup for JSONL
    #   storage.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _pending_jsonl(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        """Perform the same pending-evaluation lookup for JSONL storage."""
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

    # Method purpose: Query SQLite for evaluation rows missing persistence
    #   metrics.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _pending_persistence_backfill_sqlite(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        """Query SQLite for evaluation rows missing persistence metrics."""
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            columns = _evaluation_columns(connection)
            required = {"PERSIST_MAE_T", "PERSIST_RMSE_T", "PERSIST_MAE_RH", "PERSIST_RMSE_RH"}
            if not required <= columns:
                return []
            query = """
                SELECT e.ts_forecast_utc, e.pod_id, e.scenario, e.MAE_T, e.RMSE_T, e.MAE_RH, e.RMSE_RH,
                       e.PERSIST_MAE_T, e.PERSIST_RMSE_T, e.PERSIST_MAE_RH, e.PERSIST_RMSE_RH,
                       e.BIAS_T, e.BIAS_RH, e.event_detected, e.large_error, e.notes,
                       f.json_forecast, f.json_p25, f.json_p75
                FROM evaluations AS e
                INNER JOIN forecasts AS f
                    ON f.pod_id = e.pod_id
                   AND f.ts_pc_utc = e.ts_forecast_utc
                   AND f.scenario = e.scenario
                WHERE e.ts_forecast_utc <= ?
                  AND (
                    e.PERSIST_MAE_T IS NULL
                    OR e.PERSIST_RMSE_T IS NULL
                    OR e.PERSIST_MAE_RH IS NULL
                    OR e.PERSIST_RMSE_RH IS NULL
                  )
            """
            parameters: list[object] = [cutoff]
            if pod_ids:
                placeholders = ",".join("?" for _ in pod_ids)
                query += f" AND e.pod_id IN ({placeholders})"
                parameters.extend(pod_ids)
            query += " ORDER BY e.ts_forecast_utc ASC, e.pod_id ASC, e.scenario ASC"
            rows = connection.execute(query, tuple(parameters)).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError:
            return []
        finally:
            connection.close()

    # Method purpose: Perform the same persistence-backfill lookup for JSONL
    #   storage.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as cutoff, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _pending_persistence_backfill_jsonl(self, *, cutoff: str, pod_ids: list[str] | None) -> list[dict[str, object]]:
        """Perform the same persistence-backfill lookup for JSONL storage."""
        forecasts = {
            (str(item["pod_id"]), str(item["ts_pc_utc"]), str(item["scenario"])): item
            for item in self._read_jsonl(self.forecasts_jsonl)
        }
        pod_filter = set(pod_ids or [])
        pending: list[dict[str, object]] = []
        for item in self._read_jsonl(self.evaluations_jsonl):
            key = (str(item["pod_id"]), str(item["ts_forecast_utc"]), str(item["scenario"]))
            if pod_filter and str(item["pod_id"]) not in pod_filter:
                continue
            if str(item["ts_forecast_utc"]) > cutoff:
                continue
            if all(item.get(column) is not None for column in ("PERSIST_MAE_T", "PERSIST_RMSE_T", "PERSIST_MAE_RH", "PERSIST_RMSE_RH")):
                continue
            forecast = forecasts.get(key)
            if forecast is None:
                continue
            pending.append({**item, **{name: forecast[name] for name in ("json_forecast", "json_p25", "json_p75")}})
        pending.sort(key=lambda item: (str(item["ts_forecast_utc"]), str(item["pod_id"]), str(item["scenario"])))
        return pending

    # JSONL upsert helper
    # - Purpose: appends or rewrites one JSONL payload while honouring the
    #   chosen logical key.
    # Method purpose: Append or replace one JSONL payload while respecting the
    #   chosen key.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as path, key_fields, payload, replace_existing,
    #   interpreted according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _append_jsonl(
        self,
        *,
        path: Path,
        key_fields: tuple[str, ...],
        payload: dict[str, object],
        replace_existing: bool = False,
    ) -> None:
        """Append or replace one JSONL payload while respecting the chosen key."""
        items = self._read_jsonl(path)
        candidate_key = tuple(str(payload[field]) for field in key_fields)
        existing = [item for item in items if tuple(str(item[field]) for field in key_fields) == candidate_key]
        if existing and not replace_existing:
            return
        if replace_existing:
            rewritten = [item for item in items if tuple(str(item[field]) for field in key_fields) != candidate_key]
            rewritten.append(payload)
            with path.open("w", encoding="utf-8", newline="\n") as handle:
                for item in rewritten:
                    handle.write(json.dumps(item, separators=(",", ":"), sort_keys=True))
                    handle.write("\n")
            return
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True))
            handle.write("\n")

    # Method purpose: Read recent trustworthy bias values from SQLite.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as pod_id, scenario, limit, interpreted according
    #   to the implementation below.
    # - Outputs: Returns RecentBias | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _recent_bias_sqlite(self, *, pod_id: str, scenario: str, limit: int) -> RecentBias | None:
        """Read recent trustworthy bias values from SQLite."""
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            columns = _evaluation_columns(connection)
            if "BIAS_T" not in columns or "BIAS_RH" not in columns:
                return None
            rows = connection.execute(
                """
                SELECT BIAS_T, BIAS_RH
                FROM evaluations
                WHERE pod_id = ?
                  AND scenario = ?
                  AND COALESCE(large_error, 0) = 0
                  AND (
                    notes IS NULL
                    OR (
                        notes NOT LIKE '%actual_missing_rate=%'
                        AND notes NOT LIKE '%forecast_missing_rate=%'
                    )
                  )
                ORDER BY ts_forecast_utc DESC
                LIMIT ?
                """,
                (str(pod_id), str(scenario), int(limit)),
            ).fetchall()
        except sqlite3.OperationalError:
            return None
        finally:
            connection.close()
        return _rows_to_recent_bias(rows)

    # Method purpose: Read recent trustworthy bias values from JSONL storage.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as pod_id, scenario, limit, interpreted according
    #   to the implementation below.
    # - Outputs: Returns RecentBias | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _recent_bias_jsonl(self, *, pod_id: str, scenario: str, limit: int) -> RecentBias | None:
        """Read recent trustworthy bias values from JSONL storage."""
        rows = [
            item
            for item in self._read_jsonl(self.evaluations_jsonl)
            if str(item.get("pod_id")) == str(pod_id)
            and str(item.get("scenario") or "") == str(scenario)
            and "BIAS_T" in item
            and "BIAS_RH" in item
            and not bool(item.get("large_error"))
            and _notes_support_calibration(str(item.get("notes") or ""))
        ]
        rows.sort(key=lambda item: str(item.get("ts_forecast_utc") or ""), reverse=True)
        return _rows_to_recent_bias(rows[:limit])

    # Method purpose: Reads JSONL for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastOutputs.
    # - Inputs: Arguments such as path, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[dict[str, object]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

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


# Scenario serialisation
# - Purpose: converts one in-memory scenario into the stable stored payload
#   expected by forecast-loading code and dashboard services.
# - Important fields:
#   - feature vector for the forecast anchor
#   - source and notes for method explanation
#   - dew-point path for direct downstream display
# Function purpose: Serialize one in-memory scenario into the stable stored payload
#   format.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as bundle, scenario, interpreted according to the
#   implementation below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _scenario_payload(bundle: ForecastBundle, scenario: ForecastTrajectory) -> dict[str, object]:
    """Serialize one in-memory scenario into the stable stored payload format."""
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


# JSON coercion
# - Purpose: lets callers handle forecast payloads regardless of whether they
#   arrived already decoded or as stored JSON text.
# Function purpose: Accept either an already-decoded dict or a stored JSON string.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _coerce_json(value: object) -> dict[str, object]:
    """Accept either an already-decoded dict or a stored JSON string."""
    if isinstance(value, dict):
        return value
    return json.loads(str(value))


# Evaluation schema migration
# - Purpose: keeps older SQLite archives compatible with newer evaluation
#   fields without requiring a separate migration tool.
# Function purpose: Lazily add newer evaluation columns to older databases when
#   needed.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as connection, interpreted according to the
#   implementation below.
# - Outputs: Returns None when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _ensure_evaluation_columns(connection: sqlite3.Connection) -> None:
    """Lazily add newer evaluation columns to older databases when needed."""
    columns = _evaluation_columns(connection)
    if "PERSIST_MAE_T" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN PERSIST_MAE_T REAL")
    if "PERSIST_RMSE_T" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN PERSIST_RMSE_T REAL")
    if "PERSIST_MAE_RH" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN PERSIST_MAE_RH REAL")
    if "PERSIST_RMSE_RH" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN PERSIST_RMSE_RH REAL")
    if "BIAS_T" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN BIAS_T REAL NOT NULL DEFAULT 0")
    if "BIAS_RH" not in columns:
        connection.execute("ALTER TABLE evaluations ADD COLUMN BIAS_RH REAL NOT NULL DEFAULT 0")


# Function purpose: Return the current evaluation-table column names.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as connection, interpreted according to the
#   implementation below.
# - Outputs: Returns set[str] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _evaluation_columns(connection: sqlite3.Connection) -> set[str]:
    """Return the current evaluation-table column names."""
    return {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(evaluations)").fetchall()
    }


# Recent-bias aggregation
# - Purpose: converts stored bias rows into a robust median summary that can be
#   applied safely as a lightweight calibration offset.
# Function purpose: Convert stored bias rows into a robust median bias summary.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as rows, interpreted according to the implementation
#   below.
# - Outputs: Returns RecentBias | None when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _rows_to_recent_bias(rows) -> RecentBias | None:
    """Convert stored bias rows into a robust median bias summary."""
    temp_biases: list[float] = []
    rh_biases: list[float] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            temp_value = row["BIAS_T"]
            rh_value = row["BIAS_RH"]
        else:
            temp_value = row.get("BIAS_T")
            rh_value = row.get("BIAS_RH")
        if temp_value is None or rh_value is None:
            continue
        temp_biases.append(float(temp_value))
        rh_biases.append(float(rh_value))
    if len(temp_biases) < 3 or len(rh_biases) < 3:
        return None
    return RecentBias(
        temp_c=float(median(temp_biases)),
        rh_pct=float(median(rh_biases)),
        sample_count=min(len(temp_biases), len(rh_biases)),
    )


# Calibration-eligibility filter
# - Purpose: excludes windows whose notes indicate missing-data issues that make
#   them unsuitable as calibration evidence.
# Function purpose: Filter out windows whose notes mark them as unsuitable for
#   calibration.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as notes, interpreted according to the implementation
#   below.
# - Outputs: Returns bool when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _notes_support_calibration(notes: str) -> bool:
    """Filter out windows whose notes mark them as unsuitable for calibration."""
    return "actual_missing_rate=" not in notes and "forecast_missing_rate=" not in notes
