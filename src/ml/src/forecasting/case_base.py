"""Persistence layer for analogue forecasting cases.

The analogue model depends on a historical memory: earlier 3-hour situations
paired with the 30-minute futures that actually followed them. This file is the
storage layer for that memory.

In viva terms, this is the project's case library. The forecaster later asks:
"Have we seen a similar warehouse situation before, and if so, what happened
next?"
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


class CaseBaseStore:
    """Load and append analogue cases in SQLite or JSONL form.

    The project supports both SQLite and JSONL so the same forecasting logic can
    run in the main integrated system and in smaller offline/test contexts.
    """

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

    def load_cases(self, *, pod_id: str, include_event_cases: bool = False) -> list[CaseRecord]:
        """Load historical cases for one pod.

        Baseline forecasting typically excludes event-labelled cases so that the
        normal analogue model is not dominated by disturbance periods.
        """
        if self.storage_backend == "sqlite":
            return self._load_cases_sqlite(pod_id=pod_id, include_event_cases=include_event_cases)
        return self._load_cases_jsonl(pod_id=pod_id, include_event_cases=include_event_cases)

    def append_case(self, case: CaseRecord) -> None:
        """Append one newly learned case after a forecast window is complete."""
        self.ensure_storage()
        if self.storage_backend == "sqlite":
            self._append_case_sqlite(case)
            return
        self._append_case_jsonl(case)

    def latest_case_timestamp(self, pod_id: str):
        """Return the newest stored case timestamp for one pod.

        The runner uses this to avoid relearning the same historical windows on
        every cycle.
        """
        if self.storage_backend == "sqlite":
            return self._latest_case_timestamp_sqlite(pod_id)
        return self._latest_case_timestamp_jsonl(pod_id)

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


def _filter_case_labels(cases: list[CaseRecord], *, include_event_cases: bool) -> list[CaseRecord]:
    """Optionally exclude disturbance-labelled cases from baseline matching."""
    if include_event_cases:
        return cases
    return [case for case in cases if (case.event_label or "none") in {"", "none"}]
