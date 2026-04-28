# File overview:
# - Responsibility: Historical Pod 1 forecast-test views for the prediction
#   dashboard.
# - Project role: Builds route-ready view models, summaries, and chart inputs from
#   loaded data.
# - Main data or concerns: View models, chart series, thresholds, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

"""Historical Pod 1 forecast-test views for the prediction dashboard.

Responsibilities:
- Builds the dedicated historical forecast-analysis card shown beside the live
  prediction view.
- Selects one suitable continuous Pod 1 session, locates completed forecast
  attempts inside that session, and reconstructs the comparison series needed
  for charting.

Project flow:
- stored raw telemetry + stored forecasts + stored evaluations -> selected
  historical session -> reconstructed comparison series -> dashboard test card

Why this matters:
- The live prediction view shows only the newest forecast.
- This module exposes a completed historical example where input history,
  forecast, persistence baseline, and realised outcome can be inspected
  together.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app.data_access.csv_reader import read_raw_samples
from app.data_access.file_finder import find_raw_pod_files
from app.data_access.forecast_reader import read_evaluation_history, read_forecasts_in_window
from app.data_access.sqlite_reader import read_raw_samples_sqlite, sqlite_db_exists
from app.services.timeseries_service import EXPECTED_SAMPLE_INTERVAL
from app.timezone import timezone_label, to_display_time


FORECAST_TEST_POD_ID = "01"
FORECAST_HISTORY_MINUTES = 180
FORECAST_HORIZON_MINUTES = 30
SESSION_BREAK_THRESHOLD = EXPECTED_SAMPLE_INTERVAL * 2


# Class purpose: One selectable completed forecast attempt within the chosen
#   session.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class ForecastTestAttemptOption:
    """One selectable completed forecast attempt within the chosen session."""
    ts_forecast_utc: datetime
    query_value: str
    label: str
    selected: bool


# Class purpose: Summary of one continuous historical session judged suitable for
#   analysis.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class ForecastTestSession:
    """Summary of one continuous historical session judged suitable for analysis."""
    start_utc: datetime
    end_utc: datetime
    duration: timedelta
    raw_reading_count: int
    grid_point_count: int
    completed_window_count: int
    gap_rate: float
    cadence_mad_seconds: float


# Class purpose: All series needed to compare one stored forecast attempt with
#   reality.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class ForecastAttemptSeries:
    """All series needed to compare one stored forecast attempt with reality."""
    history_times_utc: list[datetime]
    history_temp_c: list[float]
    history_rh_pct: list[float]
    future_times_utc: list[datetime]
    actual_temp_c: list[float]
    actual_rh_pct: list[float]
    forecast_temp_c: list[float]
    forecast_rh_pct: list[float]
    persistence_temp_c: list[float]
    persistence_rh_pct: list[float]
    anchor_temp_c: float
    anchor_rh_pct: float


# Class purpose: Full view model consumed by the historical forecast-test dashboard
#   card.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class PodForecastTestView:
    """Full view model consumed by the historical forecast-test dashboard card."""
    title: str
    session: ForecastTestSession
    session_definition: str
    selected_attempt_ts_utc: datetime
    selected_attempt_source: str
    selected_attempt_neighbor_count: int
    selected_attempt_case_count: int
    selected_attempt_missing_rate: float
    selected_attempt_notes: str
    rmse_temp_c: float | None
    rmse_rh_pct: float | None
    persist_rmse_temp_c: float | None
    persist_rmse_rh_pct: float | None
    temp_rmse_advantage_c: float | None
    rh_rmse_advantage_pct: float | None
    large_error: bool
    evaluation_notes: str
    attempt_options: list[ForecastTestAttemptOption]
    previous_attempt_query: str | None
    next_attempt_query: str | None
    session_chart: str | None
    detail_chart: str | None
    uses_stored_completed_forecasts: bool
    uses_reconstructed_actual: bool
    uses_reconstructed_persistence: bool


# Historical forecast-test context builder
# - Purpose: assembles the full dashboard view model for the dedicated Pod 1
#   historical forecast-analysis card.
# - Project role: top-level service entry point for completed-forecast
#   inspection.
# - Outputs: one ``PodForecastTestView`` containing session summaries,
#   selectable attempts, comparison metrics, and charts.
# Function purpose: Build the historical forecast-analysis card for Pod 1.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, display_timezone,
#   selected_attempt_ts, interpreted according to the implementation below.
# - Outputs: Returns PodForecastTestView | None when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def build_pod1_forecast_test_context(
    data_root: Path,
    *,
    db_path: Path | None = None,
    display_timezone: tzinfo | None = None,
    selected_attempt_ts: str | None = None,
) -> PodForecastTestView | None:
    """Build the historical forecast-analysis card for Pod 1."""
    resolved_display_timezone = display_timezone or timezone.utc
    raw_frame = _load_raw_frame(Path(data_root), pod_id=FORECAST_TEST_POD_ID, db_path=db_path)
    if raw_frame.empty:
        return None

    minute_frame = _aggregate_minute_frame(raw_frame)
    attempts = _load_completed_attempts(Path(data_root), raw_frame=raw_frame, db_path=db_path)
    if minute_frame.empty or attempts.empty:
        return None

    session = _select_best_session(minute_frame, attempts)
    if session is None:
        return None

    session_attempts = attempts[
        attempts["ts_pc_utc"].between(pd.Timestamp(session.start_utc), pd.Timestamp(session.end_utc), inclusive="both")
    ].copy()
    if session_attempts.empty:
        return None

    selected_attempt = _select_attempt(session_attempts, selected_attempt_ts=selected_attempt_ts, session=session)
    if selected_attempt is None:
        return None

    selected_timestamp = selected_attempt["ts_pc_utc"].to_pydatetime()
    attempt_series = _reconstruct_attempt_series(minute_frame, selected_attempt)
    session_chart = _build_session_overview_chart(
        minute_frame=minute_frame,
        attempts=session_attempts,
        session=session,
        selected_attempt_ts=selected_timestamp,
        display_timezone=resolved_display_timezone,
    )
    detail_chart = _build_forecast_detail_chart(
        attempt=selected_attempt,
        attempt_series=attempt_series,
        display_timezone=resolved_display_timezone,
    )

    options = _build_attempt_options(
        session_attempts=session_attempts,
        selected_timestamp=selected_timestamp,
        display_timezone=resolved_display_timezone,
    )
    selected_index = next(
        (index for index, option in enumerate(options) if option.query_value == _to_utc_iso(selected_timestamp)),
        0,
    )
    previous_attempt_query = options[selected_index - 1].query_value if selected_index > 0 else None
    next_attempt_query = options[selected_index + 1].query_value if selected_index + 1 < len(options) else None

    rmse_temp_c = _optional_float(selected_attempt.get("RMSE_T"))
    rmse_rh_pct = _optional_float(selected_attempt.get("RMSE_RH"))
    persist_rmse_temp_c = _optional_float(selected_attempt.get("PERSIST_RMSE_T"))
    if persist_rmse_temp_c is None:
        persist_rmse_temp_c = _rmse(
            predicted=attempt_series.persistence_temp_c,
            actual=attempt_series.actual_temp_c,
        )
    persist_rmse_rh_pct = _optional_float(selected_attempt.get("PERSIST_RMSE_RH"))
    if persist_rmse_rh_pct is None:
        persist_rmse_rh_pct = _rmse(
            predicted=attempt_series.persistence_rh_pct,
            actual=attempt_series.actual_rh_pct,
        )
    temp_advantage = None if rmse_temp_c is None or persist_rmse_temp_c is None else persist_rmse_temp_c - rmse_temp_c
    rh_advantage = None if rmse_rh_pct is None or persist_rmse_rh_pct is None else persist_rmse_rh_pct - rmse_rh_pct

    return PodForecastTestView(
        title="Pod 1 Forecasting Test",
        session=session,
        session_definition=(
            "Continuous sessions are built on the forecasting pipeline's 1-minute resampled grid. "
            "A session continues while adjacent populated grid minutes stay within 2 minutes of each other; "
            "a larger gap starts a new session."
        ),
        selected_attempt_ts_utc=selected_timestamp,
        selected_attempt_source=str(selected_attempt.get("source") or "unknown"),
        selected_attempt_neighbor_count=int(selected_attempt.get("neighbor_count") or 0),
        selected_attempt_case_count=int(selected_attempt.get("case_count") or 0),
        selected_attempt_missing_rate=float(selected_attempt.get("forecast_missing_rate") or 0.0),
        selected_attempt_notes=str(selected_attempt.get("forecast_notes") or ""),
        rmse_temp_c=rmse_temp_c,
        rmse_rh_pct=rmse_rh_pct,
        persist_rmse_temp_c=persist_rmse_temp_c,
        persist_rmse_rh_pct=persist_rmse_rh_pct,
        temp_rmse_advantage_c=temp_advantage,
        rh_rmse_advantage_pct=rh_advantage,
        large_error=bool(selected_attempt.get("large_error")) if not pd.isna(selected_attempt.get("large_error")) else False,
        evaluation_notes=str(selected_attempt.get("evaluation_notes") or ""),
        attempt_options=options,
        previous_attempt_query=previous_attempt_query,
        next_attempt_query=next_attempt_query,
        session_chart=session_chart,
        detail_chart=detail_chart,
        uses_stored_completed_forecasts=True,
        uses_reconstructed_actual=True,
        uses_reconstructed_persistence=True,
    )


# Completed-attempt loader
# - Purpose: returns stored baseline forecast rows that already have completed
#   evaluations.
# - Project role: historical evidence read path for the test card.
# - Important decision: only completed windows are eligible, so every selected
#   attempt has both stored prediction output and realised outcome metrics.
# Function purpose: Load baseline forecast attempts that also have completed
#   evaluations.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, raw_frame, db_path, interpreted according
#   to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _load_completed_attempts(
    data_root: Path,
    *,
    raw_frame: pd.DataFrame,
    db_path: Path | None,
) -> pd.DataFrame:
    """Load baseline forecast attempts that also have completed evaluations."""
    window_start = _to_utc_iso(raw_frame["ts_pc_utc"].min().to_pydatetime())
    window_end = _to_utc_iso(raw_frame["ts_pc_utc"].max().to_pydatetime() + EXPECTED_SAMPLE_INTERVAL)
    forecasts = read_forecasts_in_window(
        db_path,
        start_utc=window_start,
        end_utc=window_end,
        pod_id=FORECAST_TEST_POD_ID,
    )
    if forecasts.empty:
        return pd.DataFrame()

    forecasts = forecasts[forecasts["scenario"] == "baseline"].copy()
    evaluations = read_evaluation_history(
        Path(data_root),
        db_path=db_path,
        pod_id=FORECAST_TEST_POD_ID,
        scenario="baseline",
    )
    if evaluations.empty:
        return pd.DataFrame()

    completed = forecasts.merge(
        evaluations[
            [
                "ts_forecast_utc",
                "pod_id",
                "scenario",
                "PERSIST_MAE_T",
                "PERSIST_RMSE_T",
                "PERSIST_MAE_RH",
                "PERSIST_RMSE_RH",
                "large_error",
                "evaluation_notes",
            ]
        ],
        left_on=["ts_pc_utc", "pod_id", "scenario"],
        right_on=["ts_forecast_utc", "pod_id", "scenario"],
        how="inner",
        suffixes=("", "_eval"),
    )
    if completed.empty:
        return completed

    completed["forecast_payload"] = completed["json_forecast"].apply(json.loads)
    completed["forecast_missing_rate"] = completed["forecast_payload"].apply(
        lambda payload: float(payload.get("missing_rate") or 0.0)
    )
    completed["source"] = completed["forecast_payload"].apply(lambda payload: str(payload.get("source") or "unknown"))
    completed["neighbor_count"] = completed["forecast_payload"].apply(lambda payload: int(payload.get("neighbor_count") or 0))
    completed["case_count"] = completed["forecast_payload"].apply(lambda payload: int(payload.get("case_count") or 0))
    completed["forecast_notes"] = completed["forecast_payload"].apply(lambda payload: str(payload.get("notes") or ""))
    completed["temp_anchor_c"] = completed["forecast_payload"].apply(
        lambda payload: float((payload.get("feature_vector") or {}).get("temp_last") or 0.0)
    )
    completed["rh_anchor_pct"] = completed["forecast_payload"].apply(
        lambda payload: float((payload.get("feature_vector") or {}).get("rh_last") or 0.0)
    )
    completed["temp_forecast_c"] = completed["forecast_payload"].apply(
        lambda payload: [float(value) for value in payload.get("temp_forecast_c") or []]
    )
    completed["rh_forecast_pct"] = completed["forecast_payload"].apply(
        lambda payload: [float(value) for value in payload.get("rh_forecast_pct") or []]
    )
    return completed.sort_values("ts_pc_utc", kind="mergesort").reset_index(drop=True)


# Session selection
# - Purpose: ranks candidate continuous sessions and chooses the strongest one
#   for the historical card.
# - Important decisions: favours sessions with more completed forecast windows,
#   then longer duration, fewer gaps, and more regular cadence.
# Function purpose: Choose the most useful continuous Pod 1 session for
#   explanation/debugging.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as minute_frame, attempts, interpreted according to the
#   implementation below.
# - Outputs: Returns ForecastTestSession | None when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _select_best_session(minute_frame: pd.DataFrame, attempts: pd.DataFrame) -> ForecastTestSession | None:
    """Choose the most useful continuous Pod 1 session for explanation/debugging."""
    sessions = _summarize_continuous_sessions(minute_frame, attempts)
    if not sessions:
        return None
    sessions_with_attempts = [session for session in sessions if session.completed_window_count > 0]
    candidates = sessions_with_attempts or sessions
    return max(
        candidates,
        key=lambda session: (
            session.completed_window_count,
            session.duration.total_seconds(),
            -session.gap_rate,
            -session.cadence_mad_seconds,
            session.grid_point_count,
        ),
    )


# Continuous-session summarisation
# - Purpose: partitions the minute grid into continuous sessions and computes
#   the quality signals later used for ranking.
# - Outputs: session start/end bounds, duration, gap rate, cadence deviation,
#   and completed forecast count.
# Function purpose: Split the minute grid into continuous sessions and score each
#   one.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as minute_frame, attempts, interpreted according to the
#   implementation below.
# - Outputs: Returns list[ForecastTestSession] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _summarize_continuous_sessions(minute_frame: pd.DataFrame, attempts: pd.DataFrame) -> list[ForecastTestSession]:
    """Split the minute grid into continuous sessions and score each one."""
    if minute_frame.empty:
        return []

    ordered = minute_frame.sort_values("ts_pc_utc", kind="mergesort").reset_index(drop=True).copy()
    # Session boundaries follow the same minute-level continuity assumption used
    # by the forecasting pipeline: a large enough gap starts a new session.
    session_breaks = ordered["ts_pc_utc"].diff().gt(pd.Timedelta(SESSION_BREAK_THRESHOLD)).fillna(False)
    ordered["session_id"] = session_breaks.cumsum()

    sessions: list[ForecastTestSession] = []
    for _, session_frame in ordered.groupby("session_id", sort=False):
        session_frame = session_frame.reset_index(drop=True)
        if session_frame.empty:
            continue
        start_utc = session_frame["ts_pc_utc"].iloc[0].to_pydatetime()
        end_utc = session_frame["ts_pc_utc"].iloc[-1].to_pydatetime()
        deltas = session_frame["ts_pc_utc"].diff().dropna()
        if deltas.empty:
            gap_rate = 0.0
            cadence_mad_seconds = 0.0
        else:
            gap_rate = float(deltas.gt(pd.Timedelta(EXPECTED_SAMPLE_INTERVAL)).mean())
            expected_seconds = EXPECTED_SAMPLE_INTERVAL.total_seconds()
            cadence_mad_seconds = float((deltas.dt.total_seconds() - expected_seconds).abs().median())
        completed_window_count = int(
            attempts["ts_pc_utc"].between(pd.Timestamp(start_utc), pd.Timestamp(end_utc), inclusive="both").sum()
        )
        sessions.append(
            ForecastTestSession(
                start_utc=start_utc,
                end_utc=end_utc,
                duration=end_utc - start_utc,
                raw_reading_count=int(session_frame["raw_count"].sum()),
                grid_point_count=int(len(session_frame)),
                completed_window_count=completed_window_count,
                gap_rate=gap_rate,
                cadence_mad_seconds=cadence_mad_seconds,
            )
        )
    return sessions


# Function purpose: Choose the forecast attempt to show by default inside the chosen
#   session.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as attempts, selected_attempt_ts, session, interpreted
#   according to the implementation below.
# - Outputs: Returns the value or side effect defined by the implementation.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _select_attempt(
    attempts: pd.DataFrame,
    *,
    selected_attempt_ts: str | None,
    session: ForecastTestSession,
):
    # Attempt selection
    # - Purpose: chooses which completed forecast attempt to display inside the
    #   selected session.
    # - Important decision: when no explicit attempt is requested, the ranking
    #   prefers lower missing-rate, no large-error flag, and a timestamp near
    #   the session midpoint.
    """Choose the forecast attempt to show by default inside the chosen session."""
    if attempts.empty:
        return None

    if selected_attempt_ts:
        requested = pd.to_datetime(selected_attempt_ts, utc=True, errors="coerce")
        if not pd.isna(requested):
            matches = attempts[attempts["ts_pc_utc"] == requested]
            if not matches.empty:
                return matches.iloc[0]

    midpoint = session.start_utc + (session.duration / 2)
    ranked = attempts.copy()
    ranked["selection_distance_s"] = ranked["ts_pc_utc"].apply(
        lambda value: abs((value.to_pydatetime() - midpoint).total_seconds())
    )
    ranked["selection_large_error"] = ranked["large_error"].fillna(0).astype(int)
    ranked = ranked.sort_values(
        ["selection_large_error", "forecast_missing_rate", "selection_distance_s", "ts_pc_utc"],
        kind="mergesort",
    )
    return ranked.iloc[0]


# Function purpose: Build the selector options that let the user move across
#   completed attempts.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as session_attempts, selected_timestamp,
#   display_timezone, interpreted according to the implementation below.
# - Outputs: Returns list[ForecastTestAttemptOption] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_attempt_options(
    *,
    session_attempts: pd.DataFrame,
    selected_timestamp: datetime,
    display_timezone: tzinfo,
) -> list[ForecastTestAttemptOption]:
    # Attempt selector options
    # - Purpose: builds the ordered dropdown/navigation options for completed
    #   forecast attempts inside the selected session.
    """Build the selector options that let the user move across completed attempts."""
    options: list[ForecastTestAttemptOption] = []
    for _, row in session_attempts.sort_values("ts_pc_utc", kind="mergesort").iterrows():
        ts_value = row["ts_pc_utc"].to_pydatetime()
        label = (
            f"{to_display_time(ts_value, display_timezone).strftime('%Y-%m-%d %H:%M:%S %Z')} | "
            f"RMSE {float(row['RMSE_T']):.2f} C / {float(row['RMSE_RH']):.2f}%"
        )
        options.append(
            ForecastTestAttemptOption(
                ts_forecast_utc=ts_value,
                query_value=_to_utc_iso(ts_value),
                label=label,
                selected=_to_utc_iso(ts_value) == _to_utc_iso(selected_timestamp),
            )
        )
    return options


# Attempt-series reconstruction
# - Purpose: rebuilds the model-input history, realised future, and persistence
#   baseline surrounding one completed forecast attempt.
# - Project role: bridge between stored summary rows and the detailed historical
#   comparison chart.
# - Downstream dependency: the detailed forecast-vs-actual chart consumes this
#   structure directly.
# Function purpose: Reconstruct history, actual future, and persistence around one
#   stored attempt.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as minute_frame, attempt, interpreted according to the
#   implementation below.
# - Outputs: Returns ForecastAttemptSeries when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _reconstruct_attempt_series(minute_frame: pd.DataFrame, attempt) -> ForecastAttemptSeries:
    """Reconstruct history, actual future, and persistence around one stored attempt."""
    ts_forecast_utc = attempt["ts_pc_utc"].to_pydatetime()
    history_start = ts_forecast_utc - timedelta(minutes=FORECAST_HISTORY_MINUTES - 1)
    future_start = ts_forecast_utc + EXPECTED_SAMPLE_INTERVAL

    history_frame = _resample_minute_grid(
        minute_frame,
        start_utc=history_start,
        periods=FORECAST_HISTORY_MINUTES,
    )
    actual_frame = _resample_minute_grid(
        minute_frame,
        start_utc=future_start,
        periods=FORECAST_HORIZON_MINUTES,
    )
    return ForecastAttemptSeries(
        history_times_utc=[value.to_pydatetime() for value in history_frame["ts_pc_utc"]],
        history_temp_c=[float(value) for value in history_frame["temp_c"]],
        history_rh_pct=[float(value) for value in history_frame["rh_pct"]],
        future_times_utc=[value.to_pydatetime() for value in actual_frame["ts_pc_utc"]],
        actual_temp_c=[float(value) for value in actual_frame["temp_c"]],
        actual_rh_pct=[float(value) for value in actual_frame["rh_pct"]],
        forecast_temp_c=[float(value) for value in attempt["temp_forecast_c"][:FORECAST_HORIZON_MINUTES]],
        forecast_rh_pct=[float(value) for value in attempt["rh_forecast_pct"][:FORECAST_HORIZON_MINUTES]],
        persistence_temp_c=[float(attempt["temp_anchor_c"]) for _ in range(FORECAST_HORIZON_MINUTES)],
        persistence_rh_pct=[float(attempt["rh_anchor_pct"]) for _ in range(FORECAST_HORIZON_MINUTES)],
        anchor_temp_c=float(attempt["temp_anchor_c"]),
        anchor_rh_pct=float(attempt["rh_anchor_pct"]),
    )


# Function purpose: Build the overview plot of the chosen historical Pod 1 session.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as minute_frame, attempts, session, selected_attempt_ts,
#   display_timezone, interpreted according to the implementation below.
# - Outputs: Returns str | None when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_session_overview_chart(
    *,
    minute_frame: pd.DataFrame,
    attempts: pd.DataFrame,
    session: ForecastTestSession,
    selected_attempt_ts: datetime,
    display_timezone: tzinfo,
) -> str | None:
    # Session overview chart
    # - Purpose: visualises the chosen continuous session and marks where
    #   completed forecast attempts occurred within it.
    """Build the overview plot of the chosen historical Pod 1 session."""
    session_frame = minute_frame[
        minute_frame["ts_pc_utc"].between(pd.Timestamp(session.start_utc), pd.Timestamp(session.end_utc), inclusive="both")
    ].copy()
    if session_frame.empty:
        return None

    figure = go.Figure()
    times = [to_display_time(value.to_pydatetime(), display_timezone) for value in session_frame["ts_pc_utc"]]
    figure.add_trace(
        go.Scatter(
            x=times,
            y=[float(value) for value in session_frame["temp_c"]],
            mode="lines",
            name="Temperature",
            line={"color": "#9a5a18", "width": 2.4},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Temperature</extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=[float(value) for value in session_frame["rh_pct"]],
            mode="lines",
            name="RH",
            line={"color": "#0f766e", "width": 2.4},
            yaxis="y2",
            hovertemplate="%{x}<br>%{y:.2f} %%<extra>RH</extra>",
        )
    )

    # Forecast markers are drawn on top of the telemetry session so the chart
    # shows where completed attempts sit within the longer operating period.
    attempt_lookup = session_frame.set_index("ts_pc_utc")
    attempt_times: list[datetime] = []
    attempt_temps: list[float] = []
    attempt_customdata: list[list[float]] = []
    for _, row in attempts.iterrows():
        ts_value = row["ts_pc_utc"]
        if ts_value in attempt_lookup.index:
            attempt_temp = float(attempt_lookup.loc[ts_value, "temp_c"])
        else:
            attempt_temp = float(row["temp_anchor_c"])
        attempt_times.append(to_display_time(ts_value.to_pydatetime(), display_timezone))
        attempt_temps.append(attempt_temp)
        attempt_customdata.append([float(row["RMSE_T"]), float(row["RMSE_RH"])])

    figure.add_trace(
        go.Scatter(
            x=attempt_times,
            y=attempt_temps,
            mode="markers",
            name="Completed forecast windows",
            marker={
                "size": [
                    12 if _to_utc_iso(row["ts_pc_utc"].to_pydatetime()) == _to_utc_iso(selected_attempt_ts) else 8
                    for _, row in attempts.iterrows()
                ],
                "symbol": "diamond",
                "color": [
                    "#c53030" if _to_utc_iso(row["ts_pc_utc"].to_pydatetime()) == _to_utc_iso(selected_attempt_ts) else "#7c5b32"
                    for _, row in attempts.iterrows()
                ],
                "line": {"width": 0},
            },
            customdata=attempt_customdata,
            hovertemplate=(
                "%{x}<br>Completed forecast window"
                "<br>RMSE temp %{customdata[0]:.2f} C"
                "<br>RMSE RH %{customdata[1]:.2f} %"
                "<extra></extra>"
            ),
        )
    )

    figure.update_layout(
        title="Selected historical Pod 1 session",
        template="plotly_white",
        margin={"l": 40, "r": 40, "t": 54, "b": 38},
        height=360,
        xaxis_title=f"Session time ({timezone_label(display_timezone, reference=session.end_utc)})",
        yaxis_title="Temperature (C)",
        yaxis2={
            "title": "Relative Humidity (%)",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )
    figure.update_xaxes(showgrid=True, gridcolor="#e9e2d4")
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4")
    return figure.to_html(full_html=False, include_plotlyjs=False, config=_plotly_chart_config())


# Function purpose: Build the detailed forecast-vs-actual chart for one completed
#   attempt.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as attempt, attempt_series, display_timezone, interpreted
#   according to the implementation below.
# - Outputs: Returns str | None when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_forecast_detail_chart(
    *,
    attempt,
    attempt_series: ForecastAttemptSeries,
    display_timezone: tzinfo,
) -> str | None:
    # Detailed comparison chart
    # - Purpose: shows the full evidence chain for one completed forecast
    #   attempt.
    # - Inputs: reconstructed input history, stored model forecast, realised
    #   future, and persistence baseline.
    # - Outputs: one two-panel Plotly chart for temperature and RH.
    """Build the detailed forecast-vs-actual chart for one completed attempt."""
    if not attempt_series.history_times_utc or not attempt_series.future_times_utc:
        return None

    anchor_time = attempt["ts_pc_utc"].to_pydatetime()
    history_times = [to_display_time(value, display_timezone) for value in attempt_series.history_times_utc]
    future_times = [to_display_time(value, display_timezone) for value in attempt_series.future_times_utc]
    anchor_display_time = to_display_time(anchor_time, display_timezone)

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=("Temperature", "Relative Humidity"),
    )

    # Function purpose: Handles line for the surrounding project flow.
    # - Project role: Belongs to the dashboard service and presentation layer
    #   and contributes one focused step within that subsystem.
    # - Inputs: Arguments such as values, anchor_value, interpreted according to
    #   the implementation below.
    # - Outputs: Returns list[float] when the function completes successfully.
    # - Design reason: Service-layer code keeps presentation decisions separate
    #   from raw data loading and lower-level runtime logic.
    # - Related flow: Consumes dashboard data-access outputs and passes rendered
    #   context to the Flask routes and templates.

    def _future_line(values: list[float], anchor_value: float) -> list[float]:
        return [anchor_value, *values]

    timeline = [anchor_display_time, *future_times]
    figure.add_trace(
        go.Scatter(
            x=history_times,
            y=attempt_series.history_temp_c,
            mode="lines",
            name="3-hour input history",
            line={"color": "#8a7a62", "width": 2},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Input history</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.forecast_temp_c, attempt_series.anchor_temp_c),
            mode="lines+markers",
            name="Model forecast",
            line={"color": "#9a5a18", "width": 2.6},
            marker={"size": 4},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Forecast</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.actual_temp_c, attempt_series.anchor_temp_c),
            mode="lines+markers",
            name="Actual next 30 min",
            line={"color": "#2563eb", "width": 2.4},
            marker={"size": 4},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Actual</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.persistence_temp_c, attempt_series.anchor_temp_c),
            mode="lines",
            name="Persistence baseline",
            line={"color": "#7f8a95", "width": 2, "dash": "dash"},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Persistence</extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=[anchor_display_time],
            y=[attempt_series.anchor_temp_c],
            mode="markers",
            name="Anchor point",
            marker={"size": 8, "color": "#2b2216"},
            hovertemplate="%{x}<br>%{y:.2f} C<extra>Anchor</extra>",
            showlegend=False,
        ),
        row=1,
        col=1,
    )

    figure.add_trace(
        go.Scatter(
            x=history_times,
            y=attempt_series.history_rh_pct,
            mode="lines",
            name="3-hour input history",
            line={"color": "#8a7a62", "width": 2},
            hovertemplate="%{x}<br>%{y:.2f} %<extra>Input history</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.forecast_rh_pct, attempt_series.anchor_rh_pct),
            mode="lines+markers",
            name="Model forecast",
            line={"color": "#0f766e", "width": 2.6},
            marker={"size": 4},
            hovertemplate="%{x}<br>%{y:.2f} %<extra>Forecast</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.actual_rh_pct, attempt_series.anchor_rh_pct),
            mode="lines+markers",
            name="Actual next 30 min",
            line={"color": "#2563eb", "width": 2.4},
            marker={"size": 4},
            hovertemplate="%{x}<br>%{y:.2f} %<extra>Actual</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=timeline,
            y=_future_line(attempt_series.persistence_rh_pct, attempt_series.anchor_rh_pct),
            mode="lines",
            name="Persistence baseline",
            line={"color": "#7f8a95", "width": 2, "dash": "dash"},
            hovertemplate="%{x}<br>%{y:.2f} %<extra>Persistence</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=[anchor_display_time],
            y=[attempt_series.anchor_rh_pct],
            mode="markers",
            name="Anchor point",
            marker={"size": 8, "color": "#2b2216"},
            hovertemplate="%{x}<br>%{y:.2f} %<extra>Anchor</extra>",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    for row_index in (1, 2):
        figure.add_vline(
            x=anchor_display_time,
            line={"color": "#cbb38b", "width": 1, "dash": "dot"},
            row=row_index,
            col=1,
        )

    figure.update_layout(
        title="Historical forecast vs actual for the selected completed window",
        template="plotly_white",
        margin={"l": 48, "r": 20, "t": 66, "b": 44},
        height=620,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )
    figure.update_xaxes(
        showgrid=True,
        gridcolor="#e9e2d4",
        title_text=f"Forecast time ({timezone_label(display_timezone, reference=anchor_time)})",
        row=2,
        col=1,
    )
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4", title_text="Temperature (C)", row=1, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4", title_text="Relative Humidity (%)", row=2, col=1)
    return figure.to_html(full_html=False, include_plotlyjs=False, config=_plotly_chart_config())


# Minute-frame aggregation
# - Purpose: collapses raw readings onto the same minute grid used by the
#   forecasting pipeline.
# - Project role: first preparation step before session detection and attempt
#   reconstruction.
# Function purpose: Collapse raw readings to the minute grid used by the forecasting
#   pipeline.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as raw_frame, interpreted according to the implementation
#   below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _aggregate_minute_frame(raw_frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse raw readings to the minute grid used by the forecasting pipeline."""
    if raw_frame.empty:
        return pd.DataFrame(columns=["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c", "raw_count"])

    frame = raw_frame.dropna(subset=["ts_pc_utc", "temp_c", "rh_pct"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c", "raw_count"])

    frame["ts_minute_utc"] = frame["ts_pc_utc"].dt.floor("min")
    aggregated = (
        frame.groupby("ts_minute_utc", as_index=False)
        .agg(
            temp_c=("temp_c", "mean"),
            rh_pct=("rh_pct", "mean"),
            dew_point_c=("dew_point_c", "mean"),
            raw_count=("ts_pc_utc", "count"),
        )
        .rename(columns={"ts_minute_utc": "ts_pc_utc"})
        .sort_values("ts_pc_utc", kind="mergesort")
        .reset_index(drop=True)
    )
    return aggregated


# Historical minute-grid reconstruction
# - Purpose: rebuilds a contiguous minute-level grid for the selected history or
#   future window.
# - Design reason: the historical card should use the same temporal basis as the
#   forecast model so the comparison remains structurally faithful.
# Function purpose: Rebuild a contiguous minute-level grid for historical charting.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as minute_frame, start_utc, periods, interpreted
#   according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _resample_minute_grid(
    minute_frame: pd.DataFrame,
    *,
    start_utc: datetime,
    periods: int,
) -> pd.DataFrame:
    """Rebuild a contiguous minute-level grid for historical charting."""
    if minute_frame.empty or periods <= 0:
        return pd.DataFrame(columns=["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c", "observed"])

    full_index = pd.date_range(start=start_utc, periods=periods, freq=f"{int(EXPECTED_SAMPLE_INTERVAL.total_seconds() // 60)}min", tz="UTC")
    indexed = minute_frame.set_index("ts_pc_utc").sort_index()
    combined = indexed.reindex(indexed.index.union(full_index)).sort_index()
    combined[["temp_c", "rh_pct", "dew_point_c"]] = (
        combined[["temp_c", "rh_pct", "dew_point_c"]]
        .interpolate(method="time", limit_direction="both")
        .ffill()
        .bfill()
    )
    result = combined.reindex(full_index).reset_index().rename(columns={"index": "ts_pc_utc"})
    observed_index = set(indexed.index)
    result["observed"] = result["ts_pc_utc"].isin(observed_index)
    return result[["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c", "observed"]]


# Raw-frame loader
# - Purpose: loads raw telemetry for the historical test card from SQLite when
#   available, otherwise from canonical raw files.
# Function purpose: Load Pod telemetry from SQLite when available, otherwise from
#   raw files.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, db_path, interpreted according to
#   the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _load_raw_frame(data_root: Path, *, pod_id: str, db_path: Path | None) -> pd.DataFrame:
    """Load Pod telemetry from SQLite when available, otherwise from raw files."""
    if db_path is not None and sqlite_db_exists(db_path):
        return read_raw_samples_sqlite(db_path, pod_id=pod_id)
    return read_raw_samples(find_raw_pod_files(data_root, pod_id))


# Function purpose: Convert optional scalar values into floats while tolerating
#   missing values.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns float | None when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _optional_float(value: object) -> float | None:
    """Convert optional scalar values into floats while tolerating missing values."""
    if value is None or pd.isna(value):
        return None
    return float(value)


# Function purpose: Compute RMSE locally for reconstructed series when needed.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as predicted, actual, interpreted according to the
#   implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _rmse(*, predicted: list[float], actual: list[float]) -> float:
    """Compute RMSE locally for reconstructed series when needed."""
    if not predicted or not actual:
        return 0.0
    count = min(len(predicted), len(actual))
    squared_error = sum((float(predicted[index]) - float(actual[index])) ** 2 for index in range(count))
    return (squared_error / float(count)) ** 0.5


# Function purpose: Serialise a UTC timestamp in the repository's standard string
#   form.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _to_utc_iso(value: datetime) -> str:
    """Serialise a UTC timestamp in the repository's standard string form."""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# Function purpose: Return the shared Plotly interaction settings used by the test
#   card.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _plotly_chart_config() -> dict[str, object]:
    """Return the shared Plotly interaction settings used by the test card."""
    return {
        "displayModeBar": True,
        "displaylogo": False,
        "responsive": True,
        "scrollZoom": True,
        "doubleClick": "reset",
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    }
