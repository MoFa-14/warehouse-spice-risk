from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = DASHBOARD_ROOT.parent.parent
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.forecast_test_service import (
    FORECAST_HISTORY_MINUTES,
    FORECAST_HORIZON_MINUTES,
    _reconstruct_attempt_series,
    _select_best_session,
    _summarize_continuous_sessions,
    build_pod1_forecast_test_context,
)


class ForecastTestServiceTests(unittest.TestCase):
    def test_select_best_session_prefers_long_clean_session_with_completed_windows(self) -> None:
        session_a = pd.date_range("2026-04-22T08:00:00Z", periods=40, freq="min")
        session_b = pd.date_range("2026-04-22T10:00:00Z", periods=196, freq="min")
        minute_frame = pd.DataFrame(
            {
                "ts_pc_utc": list(session_a) + list(session_b),
                "temp_c": [20.0] * len(session_a) + [24.0] * len(session_b),
                "rh_pct": [55.0] * len(session_a) + [34.0] * len(session_b),
                "dew_point_c": [10.0] * (len(session_a) + len(session_b)),
                "raw_count": [1] * (len(session_a) + len(session_b)),
            }
        )
        attempts = pd.DataFrame(
            {
                "ts_pc_utc": pd.to_datetime(
                    [
                        "2026-04-22T08:20:00Z",
                        "2026-04-22T10:30:00Z",
                        "2026-04-22T11:00:00Z",
                        "2026-04-22T12:15:00Z",
                    ],
                    utc=True,
                )
            }
        )

        session = _select_best_session(minute_frame, attempts)

        self.assertIsNotNone(session)
        assert session is not None
        self.assertEqual(session.start_utc, session_b[0].to_pydatetime())
        self.assertEqual(session.end_utc, session_b[-1].to_pydatetime())
        self.assertEqual(session.completed_window_count, 3)
        self.assertEqual(session.grid_point_count, 196)

    def test_session_summary_prefers_lower_gap_rate_when_duration_matches(self) -> None:
        clean = pd.date_range("2026-04-22T08:00:00Z", periods=121, freq="min")
        gappy = list(pd.date_range("2026-04-22T12:00:00Z", periods=60, freq="min"))
        gappy += list(pd.date_range("2026-04-22T13:01:00Z", periods=60, freq="min"))
        minute_frame = pd.DataFrame(
            {
                "ts_pc_utc": list(clean) + gappy,
                "temp_c": [22.0] * (len(clean) + len(gappy)),
                "rh_pct": [45.0] * (len(clean) + len(gappy)),
                "dew_point_c": [9.0] * (len(clean) + len(gappy)),
                "raw_count": [1] * (len(clean) + len(gappy)),
            }
        )
        attempts = pd.DataFrame(
            {"ts_pc_utc": pd.to_datetime(["2026-04-22T08:30:00Z", "2026-04-22T12:30:00Z"], utc=True)}
        )

        sessions = _summarize_continuous_sessions(minute_frame, attempts)

        self.assertEqual(len(sessions), 2)
        self.assertLess(sessions[0].gap_rate, sessions[1].gap_rate)
        self.assertEqual(sessions[0].duration, sessions[1].duration)

    def test_reconstruct_attempt_series_uses_full_history_window_and_persistence_anchor(self) -> None:
        timestamps = pd.date_range("2026-04-23T00:00:00Z", periods=260, freq="min")
        minute_frame = pd.DataFrame(
            {
                "ts_pc_utc": timestamps,
                "temp_c": [20.0 + 0.02 * index for index in range(len(timestamps))],
                "rh_pct": [55.0 - 0.05 * index for index in range(len(timestamps))],
                "dew_point_c": [10.0] * len(timestamps),
                "raw_count": [1] * len(timestamps),
            }
        )
        attempt_ts = pd.Timestamp("2026-04-23T03:00:00Z")
        attempt = pd.Series(
            {
                "ts_pc_utc": attempt_ts,
                "temp_anchor_c": 23.6,
                "rh_anchor_pct": 46.0,
                "temp_forecast_c": [23.7 + 0.01 * index for index in range(FORECAST_HORIZON_MINUTES)],
                "rh_forecast_pct": [45.9 - 0.03 * index for index in range(FORECAST_HORIZON_MINUTES)],
            }
        )

        series = _reconstruct_attempt_series(minute_frame, attempt)

        self.assertEqual(len(series.history_times_utc), FORECAST_HISTORY_MINUTES)
        self.assertEqual(len(series.future_times_utc), FORECAST_HORIZON_MINUTES)
        self.assertEqual(series.history_times_utc[0], datetime(2026, 4, 23, 0, 1, tzinfo=timezone.utc))
        self.assertEqual(series.history_times_utc[-1], datetime(2026, 4, 23, 3, 0, tzinfo=timezone.utc))
        self.assertEqual(series.future_times_utc[0], datetime(2026, 4, 23, 3, 1, tzinfo=timezone.utc))
        self.assertTrue(all(value == 23.6 for value in series.persistence_temp_c))
        self.assertTrue(all(value == 46.0 for value in series.persistence_rh_pct))

    def test_build_pod1_forecast_test_context_loads_requested_completed_attempt(self) -> None:
        temp_parent = WORKSPACE_ROOT / ".tmp-tests"
        temp_parent.mkdir(parents=True, exist_ok=True)
        temp_root = temp_parent / f"forecast-test-{uuid4().hex}"
        temp_root.mkdir(parents=True, exist_ok=True)
        try:
            data_root, db_path = self._create_forecast_test_fixture(temp_root)
            selected_attempt = "2026-04-23T20:30:00Z"

            context = build_pod1_forecast_test_context(
                data_root,
                db_path=db_path,
                display_timezone=timezone.utc,
                selected_attempt_ts=selected_attempt,
            )

            self.assertIsNotNone(context)
            assert context is not None
            self.assertEqual(context.title, "Pod 1 Forecasting Test")
            self.assertEqual(
                context.selected_attempt_ts_utc,
                datetime(2026, 4, 23, 20, 30, tzinfo=timezone.utc),
            )
            self.assertEqual(context.session.completed_window_count, 3)
            self.assertTrue(context.uses_stored_completed_forecasts)
            self.assertTrue(context.uses_reconstructed_actual)
            self.assertTrue(context.uses_reconstructed_persistence)
            self.assertIsNotNone(context.session_chart)
            self.assertIsNotNone(context.detail_chart)
            self.assertIn("Selected historical Pod 1 session", context.session_chart)
            self.assertIn("Historical forecast vs actual for the selected completed window", context.detail_chart)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    @staticmethod
    def _create_forecast_test_fixture(base: Path) -> tuple[Path, Path]:
        data_root = base / "data"
        (data_root / "db").mkdir(parents=True, exist_ok=True)
        db_path = data_root / "db" / "telemetry.sqlite"
        start = datetime(2026, 4, 23, 18, 4, tzinfo=timezone.utc)
        total_minutes = 226
        raw_rows = []
        for index in range(total_minutes):
            ts_value = start + timedelta(minutes=index)
            temp_c = 24.0 + 0.02 * index
            rh_pct = 38.0 - 0.04 * index
            raw_rows.append(
                (
                    ts_value.isoformat().replace("+00:00", "Z"),
                    "01",
                    0,
                    index + 1,
                    float(index * 60),
                    temp_c,
                    rh_pct,
                    0,
                    -45,
                    "",
                    "BLE",
                )
            )

        forecast_timestamps = [
            datetime(2026, 4, 23, 19, 45, tzinfo=timezone.utc),
            datetime(2026, 4, 23, 20, 30, tzinfo=timezone.utc),
            datetime(2026, 4, 23, 21, 0, tzinfo=timezone.utc),
        ]

        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                CREATE TABLE samples_raw (
                    ts_pc_utc TEXT NOT NULL,
                    pod_id TEXT NOT NULL,
                    session_id INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    ts_uptime_s REAL,
                    temp_c REAL,
                    rh_pct REAL,
                    flags INTEGER,
                    rssi INTEGER,
                    quality_flags TEXT,
                    source TEXT,
                    PRIMARY KEY (pod_id, session_id, seq)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE forecasts (
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
            )
            connection.execute(
                """
                CREATE TABLE evaluations (
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
                    event_detected INTEGER NOT NULL,
                    large_error INTEGER NOT NULL,
                    notes TEXT,
                    PRIMARY KEY (pod_id, ts_forecast_utc, scenario)
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO samples_raw (
                    ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                raw_rows,
            )

            for attempt_ts in forecast_timestamps:
                step = int((attempt_ts - start).total_seconds() // 60)
                anchor_temp = 24.0 + 0.02 * step
                anchor_rh = 38.0 - 0.04 * step
                forecast_payload = {
                    "temp_forecast_c": [anchor_temp + 0.03 * minute for minute in range(1, 31)],
                    "rh_forecast_pct": [anchor_rh - 0.05 * minute for minute in range(1, 31)],
                    "dew_point_forecast_c": [10.0 for _ in range(30)],
                    "feature_vector": {"temp_last": anchor_temp, "rh_last": anchor_rh},
                    "missing_rate": 0.0,
                    "source": "analogue_knn",
                    "neighbor_count": 6,
                    "case_count": 6,
                    "notes": "Historical test fixture.",
                }
                band_low = {
                    "temp_c": [value - 0.2 for value in forecast_payload["temp_forecast_c"]],
                    "rh_pct": [value - 0.8 for value in forecast_payload["rh_forecast_pct"]],
                }
                band_high = {
                    "temp_c": [value + 0.2 for value in forecast_payload["temp_forecast_c"]],
                    "rh_pct": [value + 0.8 for value in forecast_payload["rh_forecast_pct"]],
                }
                connection.execute(
                    """
                    INSERT INTO forecasts (
                        ts_pc_utc, pod_id, scenario, horizon_min, json_forecast, json_p25, json_p75,
                        event_detected, event_type, event_reason, model_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_ts.isoformat().replace("+00:00", "Z"),
                        "01",
                        "baseline",
                        30,
                        json.dumps(forecast_payload),
                        json.dumps(band_low),
                        json.dumps(band_high),
                        0,
                        "none",
                        "fixture",
                        "forecasting-v1",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO evaluations (
                        ts_forecast_utc, pod_id, scenario, MAE_T, RMSE_T, MAE_RH, RMSE_RH,
                        PERSIST_MAE_T, PERSIST_RMSE_T, PERSIST_MAE_RH, PERSIST_RMSE_RH,
                        event_detected, large_error, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_ts.isoformat().replace("+00:00", "Z"),
                        "01",
                        "baseline",
                        0.18,
                        0.24,
                        0.95,
                        1.10,
                        0.24,
                        0.31,
                        1.05,
                        1.24,
                        0,
                        0,
                        "ok",
                    ),
                )
            connection.commit()
        return data_root, db_path


if __name__ == "__main__":
    unittest.main()
