# File overview:
# - Responsibility: Case-base persistence for the analogue forecasting subsystem.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Case-base persistence for the analogue forecasting subsystem.

Responsibilities:
- Stores historical 3-hour feature windows together with the 30-minute futures
  that followed them.
- Sits between forecast evaluation/learning and later analogue matching.
- Supports SQLite for the integrated runtime and JSONL for smaller offline or
  test-oriented runs.

Project flow:
- completed history window -> feature vector + realised future -> case storage
- current feature vector -> case retrieval -> neighbour matching -> forecast

Data handled:
- Forecast anchor timestamp per pod
- Interpretable feature vectors
- Future temperature and RH trajectories
- Event labels used to separate disturbance cases from baseline cases

Downstream dependency:
- ``gateway.forecast.runner.ForecastRunner`` appends new matured cases and uses
  the latest stored timestamp to avoid relearning the same windows.
- ``forecasting.knn_forecaster.AnalogueKNNForecaster`` depends on loaded cases
  as the historical memory for similarity search.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from forecasting.models import CaseRecord
from forecasting.utils import parse_utc


CASE_BASE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS case_base (
        ts_pc_utc TEXT NOT NULL,
        pod_id TEXT NOT NULL,
        feature_json TEXT NOT NULL,
        future_temp_json TEXT NOT NULL,
        future_rh_json TEXT NOT NULL,
        event_label TEXT,
        PRIMARY KEY (pod_id, ts_pc_utc)
    )
"""


# Case-base storage adapter
# - Purpose: persists the analogue memory used by the forecast model.
# - Project role: forms the storage boundary between learned historical windows
#   and later neighbour retrieval.
# - Inputs: storage backend selection plus SQLite or JSONL locations.
# - Outputs: ``CaseRecord`` collections, append operations, and latest learned
#   timestamps.
# - Design reason: the forecasting pipeline should not care whether cases are
#   stored in the main runtime database or in a lightweight file backend.
# Class purpose: Load and append analogue cases in SQLite or JSONL form.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

class CaseBaseStore:
    """Load and append analogue cases in SQLite or JSONL form."""

    # Method purpose: Handles init for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as storage_backend, sqlite_db_path, jsonl_path,
    #   interpreted according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def __init__(
        self,
        *,
        storage_backend: str,
        sqlite_db_path: Path | None = None,
        jsonl_path: Path | None = None,
    ) -> None:
        self.storage_backend = storage_backend.strip().lower()
        self.sqlite_db_path = sqlite_db_path
        self.jsonl_path = jsonl_path

    # Storage initialisation
    # - Purpose: creates the durable structure needed to store learned cases.
    # - Project role: first persistence step before the runner can append cases.
    # - Outputs: an existing SQLite table or JSONL file ready for use.
    # Method purpose: Create the backing storage for the case base if it does
    #   not exist.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def ensure_storage(self) -> None:
        """Create the backing storage for the case base if it does not exist."""
        if self.storage_backend == "sqlite":
            if self.sqlite_db_path is None:
                raise ValueError("sqlite_db_path is required for SQLite case storage.")
            connection = sqlite3.connect(self.sqlite_db_path)
            try:
                connection.execute(CASE_BASE_TABLE_SQL)
                connection.commit()
            finally:
                connection.close()
            return

        if self.jsonl_path is None:
            raise ValueError("jsonl_path is required for JSONL case storage.")
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self.jsonl_path.touch(exist_ok=True)

    # Case retrieval
    # - Purpose: returns historical analogue cases for one pod.
    # - Project role: read path used immediately before neighbour matching.
    # - Inputs: pod identifier and an optional choice to keep or exclude event
    #   cases.
    # - Outputs: ordered ``CaseRecord`` objects ready for distance scoring.
    # - Important decision: baseline forecasting normally excludes disturbance
    #   cases so ordinary matching is not dominated by abnormal windows.
    # Method purpose: Load historical cases for one pod.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, include_event_cases, interpreted
    #   according to the implementation below.
    # - Outputs: Returns list[CaseRecord] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def load_cases(self, *, pod_id: str, include_event_cases: bool = False) -> list[CaseRecord]:
        """Load historical cases for one pod."""
        if self.storage_backend == "sqlite":
            return self._load_cases_sqlite(pod_id=pod_id, include_event_cases=include_event_cases)
        return self._load_cases_jsonl(pod_id=pod_id, include_event_cases=include_event_cases)

    # Case learning write path
    # - Purpose: stores one newly matured historical example.
    # - Project role: write path used after a forecast window has fully played
    #   out and can be converted into a reusable analogue case.
    # - Inputs: one complete ``CaseRecord``.
    # - Downstream dependency: future baseline forecasts may use the appended
    #   case as a nearest neighbour.
    # Method purpose: Append one newly learned case after a forecast window is
    #   complete.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as case, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def append_case(self, case: CaseRecord) -> None:
        """Append one newly learned case after a forecast window is complete."""
        self.ensure_storage()
        if self.storage_backend == "sqlite":
            self._append_case_sqlite(case)
            return
        self._append_case_jsonl(case)

    # Incremental learning checkpoint
    # - Purpose: returns the newest learned case timestamp for one pod.
    # - Project role: prevents the runner from walking over the same historical
    #   windows every cycle.
    # - Outputs: the latest known case timestamp or ``None`` when no case has
    #   been stored yet.
    # Method purpose: Return the newest stored case timestamp for one pod.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns the value or side effect defined by the implementation.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def latest_case_timestamp(self, pod_id: str):
        """Return the newest stored case timestamp for one pod."""
        if self.storage_backend == "sqlite":
            return self._latest_case_timestamp_sqlite(pod_id)
        return self._latest_case_timestamp_jsonl(pod_id)

    # SQLite read path
    # - Purpose: reads stored analogue cases from the integrated runtime
    #   database.
    # - Important decision: rows are always ordered by forecast timestamp so the
    #   case history remains chronologically stable across runs.
    # Method purpose: Loads cases SQLite for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, include_event_cases, interpreted
    #   according to the implementation below.
    # - Outputs: Returns list[CaseRecord] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _load_cases_sqlite(self, *, pod_id: str, include_event_cases: bool) -> list[CaseRecord]:
        if self.sqlite_db_path is None or not self.sqlite_db_path.exists():
            return []
        connection = sqlite3.connect(self.sqlite_db_path)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                """
                SELECT ts_pc_utc, pod_id, feature_json, future_temp_json, future_rh_json, event_label
                FROM case_base
                WHERE pod_id = ?
                ORDER BY ts_pc_utc ASC
                """,
                (str(pod_id),),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        finally:
            connection.close()

        cases = [_row_to_case(row) for row in rows]
        return _filter_case_labels(cases, include_event_cases=include_event_cases)

    # SQLite write path
    # - Purpose: inserts one learned case into the integrated runtime database.
    # - Important decision: ``INSERT OR IGNORE`` preserves idempotence when the
    #   runner revisits the same completed window.
    # Method purpose: Appends case SQLite for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as case, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _append_case_sqlite(self, case: CaseRecord) -> None:
        assert self.sqlite_db_path is not None
        connection = sqlite3.connect(self.sqlite_db_path)
        try:
            connection.execute(CASE_BASE_TABLE_SQL)
            connection.execute(
                """
                INSERT OR IGNORE INTO case_base (
                    ts_pc_utc, pod_id, feature_json, future_temp_json, future_rh_json, event_label
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    case.ts_pc_utc,
                    case.pod_id,
                    json.dumps(case.feature_vector, separators=(",", ":"), sort_keys=True),
                    json.dumps(case.future_temp_c, separators=(",", ":")),
                    json.dumps(case.future_rh_pct, separators=(",", ":")),
                    case.event_label,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    # SQLite checkpoint lookup
    # - Purpose: reads the most recent stored case timestamp for incremental
    #   learning.
    # Method purpose: Retrieves the latest case timestamp SQLite for the calling
    #   code.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns the value or side effect defined by the implementation.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _latest_case_timestamp_sqlite(self, pod_id: str):
        if self.sqlite_db_path is None or not self.sqlite_db_path.exists():
            return None
        connection = sqlite3.connect(self.sqlite_db_path)
        try:
            row = connection.execute(
                """
                SELECT MAX(ts_pc_utc) AS latest_ts
                FROM case_base
                WHERE pod_id = ?
                """,
                (str(pod_id),),
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        finally:
            connection.close()
        if row is None or row[0] is None:
            return None
        return parse_utc(str(row[0]))

    # JSONL read path
    # - Purpose: mirrors the SQLite case loader for lightweight storage.
    # - Project role: keeps the forecasting package usable in offline contexts
    #   that do not depend on the gateway database schema.
    # Method purpose: Loads cases JSONL for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, include_event_cases, interpreted
    #   according to the implementation below.
    # - Outputs: Returns list[CaseRecord] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _load_cases_jsonl(self, *, pod_id: str, include_event_cases: bool) -> list[CaseRecord]:
        if self.jsonl_path is None or not self.jsonl_path.exists():
            return []
        cases: list[CaseRecord] = []
        with self.jsonl_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = json.loads(line)
                if str(payload.get("pod_id")) != str(pod_id):
                    continue
                cases.append(
                    CaseRecord(
                        ts_pc_utc=str(payload["ts_pc_utc"]),
                        pod_id=str(payload["pod_id"]),
                        feature_vector={key: float(value) for key, value in payload["feature_vector"].items()},
                        future_temp_c=[float(value) for value in payload["future_temp_c"]],
                        future_rh_pct=[float(value) for value in payload["future_rh_pct"]],
                        event_label=str(payload.get("event_label") or "none"),
                    )
                )
        return _filter_case_labels(cases, include_event_cases=include_event_cases)

    # JSONL write path
    # - Purpose: appends one learned case to the line-oriented fallback store.
    # Method purpose: Appends case JSONL for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as case, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _append_case_jsonl(self, case: CaseRecord) -> None:
        assert self.jsonl_path is not None
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts_pc_utc": case.ts_pc_utc,
            "pod_id": case.pod_id,
            "feature_vector": case.feature_vector,
            "future_temp_c": case.future_temp_c,
            "future_rh_pct": case.future_rh_pct,
            "event_label": case.event_label,
        }
        with self.jsonl_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True))
            handle.write("\n")

    # JSONL checkpoint lookup
    # - Purpose: mirrors the SQLite latest-case lookup for file storage.
    # Method purpose: Retrieves the latest case timestamp JSONL for the calling
    #   code.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on CaseBaseStore.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns the value or side effect defined by the implementation.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _latest_case_timestamp_jsonl(self, pod_id: str):
        if self.jsonl_path is None or not self.jsonl_path.exists():
            return None
        latest = None
        with self.jsonl_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if str(payload.get("pod_id")) != str(pod_id):
                    continue
                ts_value = parse_utc(str(payload["ts_pc_utc"]))
                if latest is None or ts_value > latest:
                    latest = ts_value
        return latest


# Row reconstruction
# - Purpose: converts one persisted SQLite row back into the in-memory
#   ``CaseRecord`` shape expected by the forecasting code.
# - Downstream dependency: called during case loading before event-label
#   filtering and neighbour selection.
# Function purpose: Convert a SQLite row back into the in-memory case structure.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as row, interpreted according to the implementation
#   below.
# - Outputs: Returns CaseRecord when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _row_to_case(row: sqlite3.Row) -> CaseRecord:
    """Convert a SQLite row back into the in-memory case structure."""
    return CaseRecord(
        ts_pc_utc=str(row["ts_pc_utc"]),
        pod_id=str(row["pod_id"]),
        feature_vector={key: float(value) for key, value in json.loads(row["feature_json"]).items()},
        future_temp_c=[float(value) for value in json.loads(row["future_temp_json"])],
        future_rh_pct=[float(value) for value in json.loads(row["future_rh_json"])],
        event_label=str(row["event_label"] or "none"),
    )


# Event-label filtering
# - Purpose: removes disturbance-labelled cases from the default baseline case
#   pool.
# - Design reason: baseline analogue matching should reflect ordinary storage
#   evolution unless an explicit caller requests event cases as well.
# Function purpose: Optionally exclude disturbance-labelled cases from baseline
#   matching.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as cases, include_event_cases, interpreted according to
#   the implementation below.
# - Outputs: Returns list[CaseRecord] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _filter_case_labels(cases: list[CaseRecord], *, include_event_cases: bool) -> list[CaseRecord]:
    """Optionally exclude disturbance-labelled cases from baseline matching."""
    if include_event_cases:
        return cases
    return [case for case in cases if (case.event_label or "none") in {"", "none"}]
