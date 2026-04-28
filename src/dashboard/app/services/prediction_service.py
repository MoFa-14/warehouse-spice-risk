# File overview:
# - Responsibility: Dashboard services for rendering stored forecasts.
# - Project role: Builds route-ready view models, summaries, and chart inputs from
#   loaded data.
# - Main data or concerns: View models, chart series, thresholds, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

"""Dashboard services for rendering stored forecasts.

Responsibilities:
- Turns stored forecast and evaluation artefacts into dashboard-ready view
  objects and Plotly charts.
- Connects the forecast archive to the prediction page, per-pod detail view,
  and historical forecast-test card.

Project flow:
- stored forecasts/evaluations -> dashboard view models -> summary cards,
  scenario explanations, and interactive charts

Why this matters:
- Forecasts are generated and stored upstream, but their operational meaning is
  communicated here through grouped scenarios, chart annotations, and summary
  text.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from datetime import tzinfo

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

from app.data_access.csv_reader import read_raw_samples
from app.data_access.file_finder import find_raw_pod_files
from app.data_access.forecast_reader import read_evaluation_history, read_latest_forecasts
from app.data_access.sqlite_reader import read_raw_samples_sqlite, sqlite_db_exists
from app.services.forecast_test_service import build_pod1_forecast_test_context
from app.services.thresholds import TrajectoryClassificationResult, classify_storage_trajectory
from app.timezone import timezone_label, to_display_time


EVENT_PERSIST_LOOKBACK_MINUTES = 5
EVENT_PERSIST_WINDOW_MINUTES = 30
EVENT_PERSIST_HORIZON_MINUTES = 30
EVENT_TEMP_RATE_CAP_C_PER_MIN = 0.30
EVENT_RH_RATE_CAP_PCT_PER_MIN = 1.00
EVENT_TEMP_BAND_C = 0.30
EVENT_RH_BAND_PCT = 1.50


# Class purpose: Compact evaluation summary attached to one displayed scenario.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class PredictionEvaluationView:
    """Compact evaluation summary attached to one displayed scenario."""
    mae_temp_c: float
    rmse_temp_c: float
    mae_rh_pct: float
    rmse_rh_pct: float
    large_error: bool
    notes: str


# Class purpose: Dashboard-ready representation of one forecast scenario.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class PredictionScenarioView:
    """Dashboard-ready representation of one forecast scenario."""
    scenario: str
    source: str
    neighbor_count: int
    case_count: int
    temp_start_c: float
    temp_end_c: float
    rh_start_pct: float
    rh_end_pct: float
    dew_start_c: float
    dew_end_c: float
    notes: str
    temp_forecast_c: list[float]
    rh_forecast_pct: list[float]
    dew_forecast_c: list[float]
    temp_p25_c: list[float]
    temp_p75_c: list[float]
    rh_p25_pct: list[float]
    rh_p75_pct: list[float]
    temp_peak_c: float
    temp_peak_minute: int
    rh_peak_pct: float
    rh_peak_minute: int
    dew_peak_c: float
    dew_peak_minute: int
    summary_headline: str
    summary_lines: tuple[str, ...]
    predicted_status: TrajectoryClassificationResult | None
    evaluation: PredictionEvaluationView | None


# Class purpose: Small card shown beneath each forecast plot to make the forecast
#   easier to explain.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class PredictionMetricSummaryCardView:
    """Small card shown beneath each forecast plot to make the forecast easier to explain."""
    metric_key: str
    scenario_key: str
    scenario_label: str
    headline: str
    supporting_line: str
    current_value: str
    end_value: str
    peak_value: str
    delta_value: str


# Class purpose: Full dashboard prediction view for one pod.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

@dataclass(frozen=True)
class PodPredictionView:
    """Full dashboard prediction view for one pod."""
    pod_id: str
    ts_pc_utc: datetime
    event_detected: bool
    event_type: str
    event_reason: str
    model_version: str
    baseline: PredictionScenarioView
    event_persist: PredictionScenarioView | None
    has_prediction: bool
    plotly_js: str = ""
    temp_chart: str | None = None
    rh_chart: str | None = None
    dew_chart: str | None = None
    comparison_chart: str | None = None
    temp_summary_cards: tuple[PredictionMetricSummaryCardView, ...] = ()
    rh_summary_cards: tuple[PredictionMetricSummaryCardView, ...] = ()
    dew_summary_cards: tuple[PredictionMetricSummaryCardView, ...] = ()

    # Method purpose: Handles chart for the surrounding project flow.
    # - Project role: Belongs to the dashboard service and presentation layer
    #   and acts as a method on PodPredictionView.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns str | None when the function completes successfully.
    # - Design reason: Service-layer code keeps presentation decisions separate
    #   from raw data loading and lower-level runtime logic.
    # - Related flow: Consumes dashboard data-access outputs and passes rendered
    #   context to the Flask routes and templates.

    @property
    def accuracy_chart(self) -> str | None:
        return self.comparison_chart


# Prediction-page context builder
# - Purpose: assembles all data required by the main prediction page.
# - Project role: top-level dashboard service entry point for the forecast
#   presentation path.
# - Inputs: forecast storage root, optional database path, selected pod, chosen
#   historical test attempt, and display timezone.
# - Outputs: page context containing grouped per-pod forecast views, the
#   selected detailed forecast view, and the historical Pod 1 test card.
# Function purpose: Build the full prediction page context.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, selected_pod_id,
#   selected_test_attempt_ts, display_timezone, interpreted according to the
#   implementation below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def build_prediction_page_context(
    data_root: Path,
    *,
    db_path: Path | None = None,
    selected_pod_id: str | None = None,
    selected_test_attempt_ts: str | None = None,
    display_timezone: tzinfo | None = None,
) -> dict[str, object]:
    """Build the full prediction page context."""
    frame = read_latest_forecasts(Path(data_root), db_path=db_path)
    evaluation_history = read_evaluation_history(Path(data_root), db_path=db_path, scenario="baseline")
    resolved_display_timezone = display_timezone or timezone.utc
    summary_predictions = _build_predictions(
        frame,
        data_root=Path(data_root),
        db_path=db_path,
        evaluation_history=evaluation_history,
        include_charts_for=set(),
        display_timezone=resolved_display_timezone,
    )
    selected = None
    predictions: list[PodPredictionView] = []
    if summary_predictions:
        selected_id = str(selected_pod_id or summary_predictions[0].pod_id)
        if all(item.pod_id != selected_id for item in summary_predictions):
            selected_id = summary_predictions[0].pod_id
        predictions = _build_predictions(
            frame,
            data_root=Path(data_root),
            db_path=db_path,
            evaluation_history=evaluation_history,
            include_charts_for={item.pod_id for item in summary_predictions},
            display_timezone=resolved_display_timezone,
        )
        predictions = _prioritize_predictions(predictions, selected_id)
        selected = next((item for item in predictions if item.pod_id == selected_id), predictions[0])
    pod1_forecast_test = build_pod1_forecast_test_context(
        Path(data_root),
        db_path=db_path,
        display_timezone=resolved_display_timezone,
        selected_attempt_ts=selected_test_attempt_ts,
    )
    return {
        "has_predictions": bool(predictions),
        "predictions": predictions,
        "selected_prediction": selected,
        "pod1_forecast_test": pod1_forecast_test,
        "plotly_js": get_plotlyjs() if predictions or pod1_forecast_test is not None else "",
    }


# Per-pod prediction context
# - Purpose: loads the latest stored forecast context for one pod.
# - Project role: detail-page service entry point when the UI focuses on a
#   single device rather than the multi-pod summary page.
# Function purpose: Build the latest forecast context for a single pod detail page.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, pod_id, db_path, display_timezone,
#   interpreted according to the implementation below.
# - Outputs: Returns PodPredictionView | None when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def build_pod_prediction_context(
    data_root: Path,
    pod_id: str,
    *,
    db_path: Path | None = None,
    display_timezone: tzinfo | None = None,
) -> PodPredictionView | None:
    """Build the latest forecast context for a single pod detail page."""
    frame = read_latest_forecasts(Path(data_root), db_path=db_path, pod_id=pod_id)
    evaluation_history = read_evaluation_history(Path(data_root), db_path=db_path, pod_id=pod_id, scenario="baseline")
    predictions = _build_predictions(
        frame,
        data_root=Path(data_root),
        db_path=db_path,
        evaluation_history=evaluation_history,
        include_charts_for={pod_id},
        display_timezone=display_timezone or timezone.utc,
    )
    return predictions[0] if predictions else None


# Stored-row to view-model conversion
# - Purpose: groups stored scenario rows into the dashboard's per-pod forecast
#   view objects.
# - Project role: main transformation stage between forecast-access tables and
#   rendered dashboard content.
# - Important decisions:
#   - baseline is the required anchor scenario for each pod
#   - event-persist remains optional and may be reconstructed only for display
#   - heavy chart generation is limited to the requested pod set
# Function purpose: Convert stored forecast rows into dashboard view objects.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as frame, data_root, db_path, evaluation_history,
#   include_charts_for, display_timezone, interpreted according to the
#   implementation below.
# - Outputs: Returns list[PodPredictionView] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_predictions(
    frame: pd.DataFrame,
    *,
    data_root: Path,
    db_path: Path | None,
    evaluation_history: pd.DataFrame,
    include_charts_for: set[str],
    display_timezone: tzinfo,
) -> list[PodPredictionView]:
    """Convert stored forecast rows into dashboard view objects."""
    if frame.empty:
        return []

    chart_pods = {str(pod_id) for pod_id in include_charts_for}
    views: list[PodPredictionView] = []
    for pod_id, pod_frame in frame.groupby("pod_id", sort=True):
        # Each pod view is anchored on the baseline scenario. Event-persist is
        # optional and may be reconstructed for display when legacy stored rows
        # are missing it.
        baseline_row = _scenario_row(pod_frame, "baseline")
        if baseline_row is None:
            continue
        baseline = _scenario_view(baseline_row)
        event_row = _scenario_row(pod_frame, "event_persist")
        event_persist = _scenario_view(event_row) if event_row is not None else None
        if event_persist is None and str(pod_id) in chart_pods:
            event_persist = _build_display_event_persist_scenario(
                data_root=data_root,
                db_path=db_path,
                pod_id=str(pod_id),
                ts_pc_utc=baseline_row["ts_pc_utc"].to_pydatetime(),
                baseline=baseline,
                event_type=str(baseline_row["event_type"] or ""),
            )
        temp_summary_cards = _build_metric_summary_cards(
            metric="temp",
            baseline=baseline,
            alternate=event_persist,
            event_type=str(baseline_row["event_type"] or ""),
        )
        rh_summary_cards = _build_metric_summary_cards(
            metric="rh",
            baseline=baseline,
            alternate=event_persist,
            event_type=str(baseline_row["event_type"] or ""),
        )
        dew_summary_cards = _build_metric_summary_cards(
            metric="dew",
            baseline=baseline,
            alternate=event_persist,
            event_type=str(baseline_row["event_type"] or ""),
        )
        temp_chart = None
        rh_chart = None
        dew_chart = None
        comparison_chart = None
        if str(pod_id) in chart_pods:
            temp_chart = _build_forecast_chart(
                ts_pc_utc=baseline_row["ts_pc_utc"].to_pydatetime(),
                baseline=baseline,
                alternate=event_persist,
                metric="temp",
                display_timezone=display_timezone,
            )
            rh_chart = _build_forecast_chart(
                ts_pc_utc=baseline_row["ts_pc_utc"].to_pydatetime(),
                baseline=baseline,
                alternate=event_persist,
                metric="rh",
                display_timezone=display_timezone,
            )
            dew_chart = _build_forecast_chart(
                ts_pc_utc=baseline_row["ts_pc_utc"].to_pydatetime(),
                baseline=baseline,
                alternate=event_persist,
                metric="dew",
                display_timezone=display_timezone,
            )
            comparison_chart = _build_persistence_comparison_chart(
                evaluation_history=evaluation_history[evaluation_history["pod_id"] == str(pod_id)],
                display_timezone=display_timezone,
            )
        views.append(
            PodPredictionView(
                pod_id=str(pod_id),
                ts_pc_utc=baseline_row["ts_pc_utc"].to_pydatetime(),
                event_detected=bool(baseline_row["event_detected"]),
                event_type=str(baseline_row["event_type"] or "none"),
                event_reason=str(baseline_row["event_reason"] or ""),
                model_version=str(baseline_row["model_version"] or ""),
                baseline=baseline,
                event_persist=event_persist,
                has_prediction=True,
                plotly_js=get_plotlyjs()
                if temp_chart is not None or rh_chart is not None or dew_chart is not None or comparison_chart is not None
                else "",
                temp_chart=temp_chart,
                rh_chart=rh_chart,
                dew_chart=dew_chart,
                comparison_chart=comparison_chart,
                temp_summary_cards=temp_summary_cards,
                rh_summary_cards=rh_summary_cards,
                dew_summary_cards=dew_summary_cards,
            )
        )
    return views


# Function purpose: Prioritizes predictions for the surrounding project flow.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as predictions, selected_pod_id, interpreted according to
#   the implementation below.
# - Outputs: Returns list[PodPredictionView] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _prioritize_predictions(predictions: list[PodPredictionView], selected_pod_id: str) -> list[PodPredictionView]:
    selected = [item for item in predictions if item.pod_id == selected_pod_id]
    remaining = [item for item in predictions if item.pod_id != selected_pod_id]
    return selected + remaining


# Function purpose: Return the first stored row for a requested scenario within one
#   pod frame.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as frame, scenario, interpreted according to the
#   implementation below.
# - Outputs: Returns the value or side effect defined by the implementation.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _scenario_row(frame: pd.DataFrame, scenario: str):
    """Return the first stored row for a requested scenario within one pod frame."""
    rows = frame[frame["scenario"] == scenario]
    if rows.empty:
        return None
    return rows.iloc[0]


# Scenario view construction
# - Purpose: expands one stored scenario row into the richer object used by the
#   prediction templates.
# - Project role: presentation preparation stage for one baseline or
#   event-persist scenario.
# - Inputs: one stored forecast row with embedded JSON payloads and optional
#   evaluation fields.
# - Outputs: ``PredictionScenarioView`` with anchor values, peak analysis,
#   summary copy, risk classification, and evaluation snippets.
# Function purpose: Convert one stored forecast row into a dashboard-friendly
#   scenario view.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as row, interpreted according to the implementation
#   below.
# - Outputs: Returns PredictionScenarioView when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _scenario_view(row) -> PredictionScenarioView:
    """Convert one stored forecast row into a dashboard-friendly scenario view."""
    if row is None:
        raise ValueError("Cannot build scenario view from an empty row.")
    forecast = json.loads(row["json_forecast"])
    p25 = json.loads(row["json_p25"])
    p75 = json.loads(row["json_p75"])
    temp_forecast_c = [float(value) for value in forecast["temp_forecast_c"]]
    rh_forecast_pct = [float(value) for value in forecast["rh_forecast_pct"]]
    dew_forecast_c = [float(value) for value in (forecast.get("dew_point_forecast_c") or [])]
    # The predicted storage classification turns the raw path into a risk-aware
    # interpretation layer that the templates can summarise directly.
    predicted_status = classify_storage_trajectory(temp_forecast_c, rh_forecast_pct)
    features = forecast.get("feature_vector") or {}
    temp_start_c = float(features.get("temp_last") or 0.0)
    rh_start_pct = float(features.get("rh_last") or 0.0)
    dew_start_c = _dew_point_anchor(features=features, temp_start_c=temp_start_c, rh_start_pct=rh_start_pct)
    # Dew point is stored when available, but a deterministic anchor fallback is
    # kept so older rows remain displayable without schema-specific branching.
    if not dew_forecast_c:
        dew_forecast_c = [dew_start_c for _ in temp_forecast_c]
    temp_peak_c, temp_peak_minute = _peak_point(temp_forecast_c)
    rh_peak_pct, rh_peak_minute = _peak_point(rh_forecast_pct)
    dew_peak_c, dew_peak_minute = _peak_point(dew_forecast_c)
    summary_headline, summary_lines = _scenario_summary_copy(
        scenario=str(row["scenario"]),
        event_type=str(row["event_type"] or ""),
        source=str(forecast.get("source") or "unknown"),
        neighbor_count=int(forecast.get("neighbor_count") or 0),
        case_count=int(forecast.get("case_count") or 0),
        temp_start_c=temp_start_c,
        temp_end_c=float(forecast["temp_forecast_c"][-1]),
        rh_start_pct=rh_start_pct,
        rh_end_pct=float(forecast["rh_forecast_pct"][-1]),
        dew_start_c=dew_start_c,
        dew_end_c=float(dew_forecast_c[-1]),
        temp_peak_c=temp_peak_c,
        temp_peak_minute=temp_peak_minute,
        rh_peak_pct=rh_peak_pct,
        rh_peak_minute=rh_peak_minute,
        dew_peak_c=dew_peak_c,
        dew_peak_minute=dew_peak_minute,
        predicted_status=predicted_status,
    )
    evaluation = None
    if not pd.isna(row["RMSE_T"]):
        evaluation = PredictionEvaluationView(
            mae_temp_c=float(row["MAE_T"]),
            rmse_temp_c=float(row["RMSE_T"]),
            mae_rh_pct=float(row["MAE_RH"]),
            rmse_rh_pct=float(row["RMSE_RH"]),
            large_error=bool(row["large_error"]) if not pd.isna(row["large_error"]) else False,
            notes=str(row["evaluation_notes"] or ""),
        )
    return PredictionScenarioView(
        scenario=str(row["scenario"]),
        source=str(forecast.get("source") or "unknown"),
        neighbor_count=int(forecast.get("neighbor_count") or 0),
        case_count=int(forecast.get("case_count") or 0),
        temp_start_c=float(features.get("temp_last") or 0.0),
        temp_end_c=float(forecast["temp_forecast_c"][-1]),
        rh_start_pct=float(features.get("rh_last") or 0.0),
        rh_end_pct=float(forecast["rh_forecast_pct"][-1]),
        dew_start_c=dew_start_c,
        dew_end_c=float(dew_forecast_c[-1]),
        notes=str(forecast.get("notes") or ""),
        temp_forecast_c=temp_forecast_c,
        rh_forecast_pct=rh_forecast_pct,
        dew_forecast_c=dew_forecast_c,
        temp_p25_c=[float(value) for value in p25["temp_c"]],
        temp_p75_c=[float(value) for value in p75["temp_c"]],
        rh_p25_pct=[float(value) for value in p25["rh_pct"]],
        rh_p75_pct=[float(value) for value in p75["rh_pct"]],
        temp_peak_c=temp_peak_c,
        temp_peak_minute=temp_peak_minute,
        rh_peak_pct=rh_peak_pct,
        rh_peak_minute=rh_peak_minute,
        dew_peak_c=dew_peak_c,
        dew_peak_minute=dew_peak_minute,
        summary_headline=summary_headline,
        summary_lines=summary_lines,
        predicted_status=predicted_status,
        evaluation=evaluation,
    )


# Forecast chart builder
# - Purpose: renders the interactive baseline and optional event-persist chart
#   for one metric.
# - Project role: final presentation stage after scenario view construction.
# - Important decisions:
#   - the first point is always the latest observed anchor
#   - uncertainty bands are shown for temperature and RH but not for dew point
#   - event-persist is visually differentiated as an alternate scenario
# Function purpose: Build one interactive forecast chart for temperature, RH, or dew
#   point.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as ts_pc_utc, baseline, alternate, metric,
#   display_timezone, interpreted according to the implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_forecast_chart(
    *,
    ts_pc_utc: datetime,
    baseline: PredictionScenarioView,
    alternate: PredictionScenarioView | None,
    metric: str,
    display_timezone: tzinfo,
) -> str:
    """Build one interactive forecast chart for temperature, RH, or dew point."""
    times = [to_display_time(ts_pc_utc + timedelta(minutes=index), display_timezone) for index in range(31)]
    figure = go.Figure()

    # Each metric reuses the same chart structure, but labels, colours, and
    # uncertainty handling differ so the combined page remains readable.
    if metric == "temp":
        anchor = baseline.temp_start_c
        baseline_mid = [anchor] + baseline.temp_forecast_c
        baseline_p25 = [anchor] + baseline.temp_p25_c
        baseline_p75 = [anchor] + baseline.temp_p75_c
        alternate_mid = None if alternate is None else [alternate.temp_start_c] + alternate.temp_forecast_c
        alternate_p25 = None if alternate is None else [alternate.temp_start_c] + alternate.temp_p25_c
        alternate_p75 = None if alternate is None else [alternate.temp_start_c] + alternate.temp_p75_c
        color = "#9a5a18"
        alt_color = "#c53030"
        title = "30-minute temperature forecast"
        y_label = "Temperature (C)"
        add_band = True
    elif metric == "rh":
        anchor = baseline.rh_start_pct
        baseline_mid = [anchor] + baseline.rh_forecast_pct
        baseline_p25 = [anchor] + baseline.rh_p25_pct
        baseline_p75 = [anchor] + baseline.rh_p75_pct
        alternate_mid = None if alternate is None else [alternate.rh_start_pct] + alternate.rh_forecast_pct
        alternate_p25 = None if alternate is None else [alternate.rh_start_pct] + alternate.rh_p25_pct
        alternate_p75 = None if alternate is None else [alternate.rh_start_pct] + alternate.rh_p75_pct
        color = "#0f766e"
        alt_color = "#dd6b20"
        title = "30-minute RH forecast"
        y_label = "Relative Humidity (%)"
        add_band = True
    else:
        anchor = baseline.dew_start_c
        baseline_mid = [anchor] + baseline.dew_forecast_c
        baseline_p25 = None
        baseline_p75 = None
        alternate_mid = None if alternate is None else [alternate.dew_start_c] + alternate.dew_forecast_c
        alternate_p25 = None
        alternate_p75 = None
        color = "#2563eb"
        alt_color = "#7c3aed"
        title = "30-minute dew point forecast"
        y_label = "Dew Point (C)"
        add_band = False

    if add_band and baseline_p25 is not None and baseline_p75 is not None:
        _add_band_trace(figure, times, baseline_p25, baseline_p75, color=color, label="Baseline band")
    figure.add_trace(
        go.Scatter(
            x=times,
            y=baseline_mid,
            mode="lines+markers",
            name="Baseline",
            line={"color": color, "width": 2.5},
            marker={"size": 4},
            hovertemplate="%{x}<br>%{y:.2f}<extra>Baseline</extra>",
        )
    )

    if alternate_mid is not None and alternate_p25 is not None and alternate_p75 is not None:
        _add_band_trace(figure, times, alternate_p25, alternate_p75, color=alt_color, label="Event-persist band", opacity=0.08)
        figure.add_trace(
            go.Scatter(
                x=times,
                y=alternate_mid,
                mode="lines+markers",
                name="Event persist",
                line={"color": alt_color, "width": 2, "dash": "dash"},
                marker={"size": 4},
                hovertemplate="%{x}<br>%{y:.2f}<extra>Event persist</extra>",
            )
        )
    elif alternate_mid is not None:
        figure.add_trace(
            go.Scatter(
                x=times,
                y=alternate_mid,
                mode="lines+markers",
                name="Event persist",
                line={"color": alt_color, "width": 2, "dash": "dash"},
                marker={"size": 4},
                hovertemplate="%{x}<br>%{y:.2f}<extra>Event persist</extra>",
            )
        )

    figure.update_layout(
        title=title,
        template="plotly_white",
        margin={"l": 36, "r": 12, "t": 48, "b": 36},
        height=320,
        xaxis_title=f"Forecast time ({timezone_label(display_timezone, reference=ts_pc_utc)})",
        yaxis_title=y_label,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )
    figure.update_xaxes(showgrid=True, gridcolor="#e9e2d4")
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4")
    return figure.to_html(full_html=False, include_plotlyjs=False, config=_plotly_chart_config())


# Persistence-comparison chart
# - Purpose: visualises how model RMSE compares with a flat persistence baseline
#   across completed forecast attempts.
# - Project role: dashboard evidence view for historical forecast skill.
# - Important decision: uses per-window RMSE advantage instead of a cumulative
#   or first-window-normalised measure, so each plotted point stands on its own
#   forecast outcome.
# Function purpose: Build the per-window model-vs-persistence comparison chart.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as evaluation_history, display_timezone, interpreted
#   according to the implementation below.
# - Outputs: Returns str | None when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_persistence_comparison_chart(*, evaluation_history: pd.DataFrame, display_timezone: tzinfo) -> str | None:
    """Build the per-window model-vs-persistence comparison chart."""
    if evaluation_history.empty:
        return None

    frame = (
        evaluation_history[["ts_forecast_utc", "RMSE_T", "RMSE_RH", "PERSIST_RMSE_T", "PERSIST_RMSE_RH"]]
        .dropna(subset=["ts_forecast_utc", "RMSE_T", "RMSE_RH", "PERSIST_RMSE_T", "PERSIST_RMSE_RH"])
        .sort_values("ts_forecast_utc", kind="mergesort")
        .reset_index(drop=True)
    )
    if frame.empty:
        return None

    frame["temp_rmse_advantage_c"] = _rmse_advantage_series(
        model_rmse=frame["RMSE_T"],
        persistence_rmse=frame["PERSIST_RMSE_T"],
    )
    frame["rh_rmse_advantage_pct"] = _rmse_advantage_series(
        model_rmse=frame["RMSE_RH"],
        persistence_rmse=frame["PERSIST_RMSE_RH"],
    )
    times = [to_display_time(ts.to_pydatetime(), display_timezone) for ts in frame["ts_forecast_utc"]]

    figure = go.Figure()
    figure.add_hline(y=0.0, line={"color": "#8a7a62", "width": 1, "dash": "dot"})
    figure.add_trace(
        go.Scatter(
            x=times,
            y=frame["temp_rmse_advantage_c"],
            mode="lines+markers",
            name="Temperature vs persistence",
            line={"color": "#9a5a18", "width": 2.5},
            marker={"size": 4},
            customdata=frame[["RMSE_T", "PERSIST_RMSE_T"]],
            hovertemplate=(
                "%{x}<br>%{y:.2f} C RMSE advantage"
                "<br>Model RMSE %{customdata[0]:.2f} C"
                "<br>Persistence RMSE %{customdata[1]:.2f} C"
                "<extra>Temperature</extra>"
            ),
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=frame["rh_rmse_advantage_pct"],
            mode="lines+markers",
            name="RH vs persistence",
            line={"color": "#0f766e", "width": 2.5},
            marker={"size": 4},
            customdata=frame[["RMSE_RH", "PERSIST_RMSE_RH"]],
            yaxis="y2",
            hovertemplate=(
                "%{x}<br>%{y:.2f} %% RH RMSE advantage"
                "<br>Model RMSE %{customdata[0]:.2f} %RH"
                "<br>Persistence RMSE %{customdata[1]:.2f} %RH"
                "<extra>Humidity</extra>"
            ),
        )
    )
    last_ts = frame["ts_forecast_utc"].iloc[-1].to_pydatetime()
    figure.update_layout(
        title="Per-window forecast RMSE advantage vs persistence",
        template="plotly_white",
        margin={"l": 36, "r": 12, "t": 48, "b": 36},
        height=320,
        xaxis_title=f"Evaluation time ({timezone_label(display_timezone, reference=last_ts)})",
        yaxis_title="Temperature RMSE advantage (C)",
        yaxis2={
            "title": "RH RMSE advantage (%)",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "zeroline": False,
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )
    figure.update_xaxes(showgrid=True, gridcolor="#e9e2d4")
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4")
    return figure.to_html(full_html=False, include_plotlyjs=False, config=_plotly_chart_config())


# Function purpose: Add a translucent uncertainty band between lower and upper
#   scenario bounds.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as figure, times, lower, upper, color, label, opacity,
#   interpreted according to the implementation below.
# - Outputs: Returns None when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _add_band_trace(
    figure: go.Figure,
    times: list[datetime],
    lower: list[float],
    upper: list[float],
    *,
    color: str,
    label: str,
    opacity: float = 0.12,
) -> None:
    """Add a translucent uncertainty band between lower and upper scenario bounds."""
    figure.add_trace(
        go.Scatter(
            x=times,
            y=upper,
            mode="lines",
            line={"width": 0},
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=times,
            y=lower,
            mode="lines",
            line={"width": 0},
            fill="tonexty",
            fillcolor=_alpha(color, opacity),
            hoverinfo="skip",
            name=label,
        )
    )


# Function purpose: Convert a hex colour into an rgba string for Plotly fill
#   styling.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as hex_color, opacity, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _alpha(hex_color: str, opacity: float) -> str:
    """Convert a hex colour into an rgba string for Plotly fill styling."""
    value = hex_color.lstrip("#")
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {opacity})"


# Function purpose: Return per-window RMSE advantage over persistence.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as model_rmse, persistence_rmse, interpreted according to
#   the implementation below.
# - Outputs: Returns pd.Series when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _rmse_advantage_series(*, model_rmse: pd.Series, persistence_rmse: pd.Series) -> pd.Series:
    """Return per-window RMSE advantage over persistence.

    Positive values mean the model beat persistence for that evaluated window.
    Negative values mean persistence was better.
    """
    if model_rmse.empty or persistence_rmse.empty:
        return pd.Series(dtype="float64")
    return persistence_rmse.astype(float) - model_rmse.astype(float)


# Metric summary-card builder
# - Purpose: creates the compact text cards that summarise scenario behaviour
#   beneath each forecast chart.
# - Outputs: one baseline card and, when present, one event-persist card.
# Function purpose: Create the text cards shown beneath each forecast chart.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, baseline, alternate, event_type, interpreted
#   according to the implementation below.
# - Outputs: Returns tuple[PredictionMetricSummaryCardView, ...] when the function
#   completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_metric_summary_cards(
    *,
    metric: str,
    baseline: PredictionScenarioView,
    alternate: PredictionScenarioView | None,
    event_type: str,
) -> tuple[PredictionMetricSummaryCardView, ...]:
    """Create the text cards shown beneath each forecast chart."""
    cards = [
        _metric_summary_card(
            metric=metric,
            scenario=baseline,
            scenario_key="baseline",
            scenario_label="Baseline",
            event_type=event_type,
        )
    ]
    if alternate is not None:
        cards.append(
            _metric_summary_card(
                metric=metric,
                scenario=alternate,
                scenario_key="event",
                scenario_label="Event persist",
                event_type=event_type,
            )
        )
    return tuple(cards)


# Single summary-card builder
# - Purpose: turns one scenario and one metric into a compact dashboard card
#   containing the headline, supporting line, and key numeric markers.
# Function purpose: Build one chart-level summary card in the more readable UI
#   format.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, scenario, scenario_key, scenario_label,
#   event_type, interpreted according to the implementation below.
# - Outputs: Returns PredictionMetricSummaryCardView when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _metric_summary_card(
    *,
    metric: str,
    scenario: PredictionScenarioView,
    scenario_key: str,
    scenario_label: str,
    event_type: str,
) -> PredictionMetricSummaryCardView:
    """Build one chart-level summary card in the more readable UI format."""
    value_start, value_end, value_peak, peak_minute = _metric_points(metric=metric, scenario=scenario)
    delta = value_end - value_start
    headline = _metric_headline(
        metric=metric,
        scenario_key=scenario_key,
        start=value_start,
        end=value_end,
        peak=value_peak,
        event_type=event_type,
    )
    supporting_line = _metric_supporting_line(
        metric=metric,
        scenario=scenario,
        scenario_key=scenario_key,
        end=value_end,
        peak=value_peak,
        peak_minute=peak_minute,
    )
    return PredictionMetricSummaryCardView(
        metric_key=metric,
        scenario_key=scenario_key,
        scenario_label=scenario_label,
        headline=headline,
        supporting_line=supporting_line,
        current_value=_format_metric_value(metric, value_start),
        end_value=_format_metric_value(metric, value_end),
        peak_value=f"{_format_metric_value(metric, value_peak)} at +{peak_minute} min",
        delta_value=_format_metric_delta(metric, delta),
    )


# Function purpose: Extract start/end/peak values for one metric from a scenario
#   view.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, scenario, interpreted according to the
#   implementation below.
# - Outputs: Returns tuple[float, float, float, int] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _metric_points(*, metric: str, scenario: PredictionScenarioView) -> tuple[float, float, float, int]:
    """Extract start/end/peak values for one metric from a scenario view."""
    if metric == "temp":
        return scenario.temp_start_c, scenario.temp_end_c, scenario.temp_peak_c, scenario.temp_peak_minute
    if metric == "rh":
        return scenario.rh_start_pct, scenario.rh_end_pct, scenario.rh_peak_pct, scenario.rh_peak_minute
    return scenario.dew_start_c, scenario.dew_end_c, scenario.dew_peak_c, scenario.dew_peak_minute


# Function purpose: Write the short plain-language headline for a metric summary
#   card.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, scenario_key, start, end, peak, event_type,
#   interpreted according to the implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _metric_headline(
    *,
    metric: str,
    scenario_key: str,
    start: float,
    end: float,
    peak: float,
    event_type: str,
) -> str:
    """Write the short plain-language headline for a metric summary card."""
    if scenario_key == "event":
        event_phrase = _humanize_event(event_type)
        lead = "If live conditions persist" if event_phrase == "live conditions" else f"If the current {event_phrase} pattern persists"
        metric_label = {
            "temp": "temperature could reach",
            "rh": "RH could reach",
            "dew": "dew point could reach",
        }[metric]
        return f"{lead}, {metric_label} {_format_metric_value(metric, peak)}."

    direction = _direction_label(metric=metric, delta=end - start)
    if metric == "dew":
        if direction == "up":
            return "Dew point rises, so the air stays more moisture-loaded."
        if direction == "down":
            return "Dew point falls, so the air becomes drier over the horizon."
        return "Dew point stays close to the current moisture level."
    if metric == "rh":
        if direction == "up":
            return "Relative humidity trends upward from the current reading."
        if direction == "down":
            return "Relative humidity eases downward over the next 30 minutes."
        return "Relative humidity stays close to the current reading."
    if direction == "up":
        return "Temperature trends upward over the next 30 minutes."
    if direction == "down":
        return "Temperature cools relative to the current anchor point."
    return "Temperature stays close to the current anchor point."


# Function purpose: Write the supporting sentence that explains source, endpoint,
#   and risk.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, scenario, scenario_key, end, peak,
#   peak_minute, interpreted according to the implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _metric_supporting_line(
    *,
    metric: str,
    scenario: PredictionScenarioView,
    scenario_key: str,
    end: float,
    peak: float,
    peak_minute: int,
) -> str:
    """Write the supporting sentence that explains source, endpoint, and risk."""
    metric_name = {
        "temp": "Temperature",
        "rh": "RH",
        "dew": "Dew point",
    }[metric]
    source_line = (
        "Analogue track"
        if scenario.source == "analogue_knn"
        else "Event-persist track"
        if scenario_key == "event"
        else str(scenario.source or "Forecast track").replace("_", " ").title()
    )
    risk_line = (
        f"Worst status {scenario.predicted_status.status.short_label} at +{scenario.predicted_status.horizon_minute} min."
        if scenario.predicted_status is not None
        else "No status classification available."
    )
    return (
        f"{source_line}: {metric_name} ends near {_format_metric_value(metric, end)}; "
        f"peak {_format_metric_value(metric, peak)} arrives at +{peak_minute} min. {risk_line}"
    )


# Function purpose: Classify a metric change as up, down, or steady for dashboard
#   copy.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, delta, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _direction_label(*, metric: str, delta: float) -> str:
    """Classify a metric change as up, down, or steady for dashboard copy."""
    threshold = 0.8 if metric == "rh" else 0.2
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "steady"


# Function purpose: Formats metric value for the surrounding project flow.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, value, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _format_metric_value(metric: str, value: float) -> str:
    if metric == "rh":
        return f"{value:.2f}%"
    return f"{value:.2f} C"


# Function purpose: Formats metric delta for the surrounding project flow.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as metric, delta, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _format_metric_delta(metric: str, delta: float) -> str:
    if metric == "rh":
        return f"{delta:+.2f}% vs now"
    return f"{delta:+.2f} C vs now"


# Display-only event-persist reconstruction
# - Purpose: recreates an event-persist scenario only for presentation when an
#   older stored forecast row did not include that alternate payload.
# - Project role: dashboard compatibility bridge for legacy forecast records.
# Function purpose: Reconstruct a display-only event-persist scenario from recent
#   raw history.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, ts_pc_utc, baseline,
#   event_type, interpreted according to the implementation below.
# - Outputs: Returns PredictionScenarioView | None when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _build_display_event_persist_scenario(
    *,
    data_root: Path,
    db_path: Path | None,
    pod_id: str,
    ts_pc_utc: datetime,
    baseline: PredictionScenarioView,
    event_type: str,
) -> PredictionScenarioView | None:
    """Reconstruct a display-only event-persist scenario from recent raw history."""
    history = _load_recent_raw_history(
        data_root=data_root,
        db_path=db_path,
        pod_id=pod_id,
        end_utc=ts_pc_utc,
        minutes=EVENT_PERSIST_WINDOW_MINUTES,
    )
    if history.empty:
        return None
    return _event_persist_from_history_frame(
        history_frame=history,
        baseline=baseline,
        event_type=event_type,
    )


# Presentation-side event-persist builder
# - Purpose: rebuilds an event-persist scenario from recent raw readings for
#   dashboard explanation only.
# - Important distinction: this path does not replace stored model output; it is
#   used only when an older forecast row lacks an alternate scenario.
# Function purpose: Build a dashboard-only event-persist scenario from recent raw
#   readings.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as history_frame, baseline, event_type, interpreted
#   according to the implementation below.
# - Outputs: Returns PredictionScenarioView | None when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _event_persist_from_history_frame(
    *,
    history_frame: pd.DataFrame,
    baseline: PredictionScenarioView,
    event_type: str,
) -> PredictionScenarioView | None:
    """Build a dashboard-only event-persist scenario from recent raw readings."""
    prepared = _prepare_recent_history_frame(history_frame)
    if prepared.empty:
        return None

    temp_last = float(prepared["temp_c"].iloc[-1])
    rh_last = float(prepared["rh_pct"].iloc[-1])
    dew_last = float(prepared["dew_point_c"].iloc[-1])
    lookback_index = max(0, len(prepared) - EVENT_PERSIST_LOOKBACK_MINUTES - 1)
    lookback_steps = max(1, len(prepared) - 1 - lookback_index)
    temp_rate = _clamp(
        (temp_last - float(prepared["temp_c"].iloc[lookback_index])) / float(lookback_steps),
        -EVENT_TEMP_RATE_CAP_C_PER_MIN,
        EVENT_TEMP_RATE_CAP_C_PER_MIN,
    )
    rh_rate = _clamp(
        (rh_last - float(prepared["rh_pct"].iloc[lookback_index])) / float(lookback_steps),
        -EVENT_RH_RATE_CAP_PCT_PER_MIN,
        EVENT_RH_RATE_CAP_PCT_PER_MIN,
    )
    temp_std = max(float(prepared["temp_c"].std(ddof=0) or 0.0), EVENT_TEMP_BAND_C)
    rh_std = max(float(prepared["rh_pct"].std(ddof=0) or 0.0), EVENT_RH_BAND_PCT)

    temp_forecast_c: list[float] = []
    rh_forecast_pct: list[float] = []
    dew_forecast_c: list[float] = []
    temp_p25_c: list[float] = []
    temp_p75_c: list[float] = []
    rh_p25_pct: list[float] = []
    rh_p75_pct: list[float] = []
    for step in range(1, EVENT_PERSIST_HORIZON_MINUTES + 1):
        widening = 1.0 + 0.5 * math.sqrt(step / float(EVENT_PERSIST_HORIZON_MINUTES))
        temp_value = temp_last + temp_rate * step
        rh_value = _clamp(rh_last + rh_rate * step, 0.0, 100.0)
        dew_value = _dew_point_c(temp_value, rh_value)
        temp_forecast_c.append(temp_value)
        rh_forecast_pct.append(rh_value)
        dew_forecast_c.append(dew_value)
        temp_p25_c.append(temp_value - temp_std * widening)
        temp_p75_c.append(temp_value + temp_std * widening)
        rh_p25_pct.append(max(0.0, rh_value - rh_std * widening))
        rh_p75_pct.append(min(100.0, rh_value + rh_std * widening))

    temp_peak_c, temp_peak_minute = _peak_point(temp_forecast_c)
    rh_peak_pct, rh_peak_minute = _peak_point(rh_forecast_pct)
    dew_peak_c, dew_peak_minute = _peak_point(dew_forecast_c)
    predicted_status = classify_storage_trajectory(temp_forecast_c, rh_forecast_pct)
    summary_headline, summary_lines = _scenario_summary_copy(
        scenario="event_persist",
        event_type=event_type,
        source="event_persist_slope",
        neighbor_count=0,
        case_count=0,
        temp_start_c=temp_last,
        temp_end_c=temp_forecast_c[-1],
        rh_start_pct=rh_last,
        rh_end_pct=rh_forecast_pct[-1],
        dew_start_c=dew_last,
        dew_end_c=dew_forecast_c[-1],
        temp_peak_c=temp_peak_c,
        temp_peak_minute=temp_peak_minute,
        rh_peak_pct=rh_peak_pct,
        rh_peak_minute=rh_peak_minute,
        dew_peak_c=dew_peak_c,
        dew_peak_minute=dew_peak_minute,
        predicted_status=predicted_status,
    )
    return PredictionScenarioView(
        scenario="event_persist",
        source="event_persist_slope",
        neighbor_count=0,
        case_count=0,
        temp_start_c=temp_last,
        temp_end_c=temp_forecast_c[-1],
        rh_start_pct=rh_last,
        rh_end_pct=rh_forecast_pct[-1],
        dew_start_c=dew_last,
        dew_end_c=dew_forecast_c[-1],
        notes=(
            "Display-only event-persist slope reconstructed from the latest recent readings "
            "because the current stored forecast did not include an alternate scenario."
        ),
        temp_forecast_c=temp_forecast_c,
        rh_forecast_pct=rh_forecast_pct,
        dew_forecast_c=dew_forecast_c,
        temp_p25_c=temp_p25_c,
        temp_p75_c=temp_p75_c,
        rh_p25_pct=rh_p25_pct,
        rh_p75_pct=rh_p75_pct,
        temp_peak_c=temp_peak_c,
        temp_peak_minute=temp_peak_minute,
        rh_peak_pct=rh_peak_pct,
        rh_peak_minute=rh_peak_minute,
        dew_peak_c=dew_peak_c,
        dew_peak_minute=dew_peak_minute,
        summary_headline=summary_headline,
        summary_lines=summary_lines,
        predicted_status=predicted_status,
        evaluation=None,
    )


# Recent raw-history loader
# - Purpose: reads the short recent telemetry slice used by the display-only
#   event-persist reconstruction path.
# Function purpose: Load a short recent raw history slice for display-side event
#   reconstruction.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, pod_id, end_utc, minutes,
#   interpreted according to the implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _load_recent_raw_history(
    *,
    data_root: Path,
    db_path: Path | None,
    pod_id: str,
    end_utc: datetime,
    minutes: int,
) -> pd.DataFrame:
    """Load a short recent raw history slice for display-side event reconstruction."""
    window_start = end_utc - timedelta(minutes=minutes + 2)
    if db_path is not None and sqlite_db_exists(db_path):
        frame = read_raw_samples_sqlite(
            db_path,
            pod_id=pod_id,
            date_from=window_start.date(),
            date_to=end_utc.date(),
        )
    else:
        frame = read_raw_samples(
            find_raw_pod_files(data_root, pod_id, date_from=window_start.date(), date_to=end_utc.date())
        )
    if frame.empty:
        return frame
    return frame[
        (frame["ts_pc_utc"] >= pd.Timestamp(window_start))
        & (frame["ts_pc_utc"] <= pd.Timestamp(end_utc))
        & frame["temp_c"].notna()
        & frame["rh_pct"].notna()
    ].copy()


# Display-side minute-grid preparation
# - Purpose: collapses recent raw readings onto the minute grid expected by the
#   display-only event-persist reconstruction.
# - Important decision: interpolation is limited to this presentation path and
#   is used only to keep the explanatory chart continuous.
# Function purpose: Collapse raw readings onto a recent minute grid for display
#   reconstruction.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as history_frame, interpreted according to the
#   implementation below.
# - Outputs: Returns pd.DataFrame when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _prepare_recent_history_frame(history_frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse raw readings onto a recent minute grid for display reconstruction."""
    if history_frame.empty:
        return pd.DataFrame(columns=["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])

    minute_frame = history_frame.copy()
    minute_frame["ts_pc_utc"] = minute_frame["ts_pc_utc"].dt.floor("min")
    minute_frame = (
        minute_frame.groupby("ts_pc_utc", as_index=False)
        .agg(temp_c=("temp_c", "mean"), rh_pct=("rh_pct", "mean"), dew_point_c=("dew_point_c", "mean"))
        .sort_values("ts_pc_utc", kind="mergesort")
        .reset_index(drop=True)
    )
    if minute_frame.empty:
        return minute_frame

    full_index = pd.date_range(
        end=minute_frame["ts_pc_utc"].iloc[-1],
        periods=min(EVENT_PERSIST_WINDOW_MINUTES, len(minute_frame)),
        freq="min",
        tz="UTC",
    )
    indexed = minute_frame.set_index("ts_pc_utc").sort_index()
    combined = indexed.reindex(indexed.index.union(full_index)).sort_index()
    combined[["temp_c", "rh_pct", "dew_point_c"]] = (
        combined[["temp_c", "rh_pct", "dew_point_c"]]
        .interpolate(method="time", limit_direction="both")
        .ffill()
        .bfill()
    )
    return combined.reindex(full_index).reset_index().rename(columns={"index": "ts_pc_utc"})


# Function purpose: Small local clamp helper used by the display-side reconstruction
#   path.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, lower, upper, interpreted according to the
#   implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _clamp(value: float, lower: float, upper: float) -> float:
    """Small local clamp helper used by the display-side reconstruction path."""
    return max(lower, min(upper, value))


# Function purpose: Return the peak value and its horizon minute for a forecast
#   metric.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the implementation
#   below.
# - Outputs: Returns tuple[float, int] when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _peak_point(values: list[float]) -> tuple[float, int]:
    """Return the peak value and its horizon minute for a forecast metric."""
    if not values:
        return 0.0, 0
    peak_index, peak_value = max(enumerate(values, start=1), key=lambda item: item[1])
    return float(peak_value), int(peak_index)


# Function purpose: Build the longer explanatory summary text shown with each
#   scenario.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as scenario, event_type, source, neighbor_count,
#   case_count, temp_start_c, temp_end_c, rh_start_pct, rh_end_pct, dew_start_c,
#   dew_end_c, temp_peak_c, temp_peak_minute, rh_peak_pct, rh_peak_minute,
#   dew_peak_c, dew_peak_minute, predicted_status, interpreted according to the
#   implementation below.
# - Outputs: Returns tuple[str, tuple[str, ...]] when the function completes
#   successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _scenario_summary_copy(
    *,
    scenario: str,
    event_type: str,
    source: str,
    neighbor_count: int,
    case_count: int,
    temp_start_c: float,
    temp_end_c: float,
    rh_start_pct: float,
    rh_end_pct: float,
    dew_start_c: float,
    dew_end_c: float,
    temp_peak_c: float,
    temp_peak_minute: int,
    rh_peak_pct: float,
    rh_peak_minute: int,
    dew_peak_c: float,
    dew_peak_minute: int,
    predicted_status: TrajectoryClassificationResult | None,
) -> tuple[str, tuple[str, ...]]:
    """Build the longer explanatory summary text shown with each scenario."""
    risk_line = _risk_summary_line(predicted_status)
    model_line = _model_summary_line(source=source, neighbor_count=neighbor_count, case_count=case_count)
    if scenario == "event_persist":
        event_phrase = _humanize_event(event_type)
        lead = (
            "If the current live conditions persist,"
            if event_phrase == "live conditions"
            else f"If the current {event_phrase} pattern persists,"
        )
        headline = (
            f"{lead} temperature could reach {temp_peak_c:.2f} C, "
            f"RH {rh_peak_pct:.2f}%, and dew point {dew_peak_c:.2f} C within 30 minutes."
        )
        lines = (
            f"The event-persist path starts from {temp_start_c:.2f} C / {rh_start_pct:.2f}% RH / {dew_start_c:.2f} C dew point "
            f"and ends near {temp_end_c:.2f} C / {rh_end_pct:.2f}% RH / {dew_end_c:.2f} C.",
            risk_line,
            model_line,
        )
        return headline, lines

    headline = (
        f"From the latest observed {temp_start_c:.2f} C / {rh_start_pct:.2f}% RH / {dew_start_c:.2f} C dew point, "
        f"the baseline path ends near {temp_end_c:.2f} C / {rh_end_pct:.2f}% RH / {dew_end_c:.2f} C in 30 minutes."
    )
    lines = (
        f"Within the next 30 minutes, temperature peaks at {temp_peak_c:.2f} C (+{temp_peak_minute} min), "
        f"RH peaks at {rh_peak_pct:.2f}% (+{rh_peak_minute} min), and dew point peaks at {dew_peak_c:.2f} C (+{dew_peak_minute} min).",
        risk_line,
        model_line,
    )
    return headline, lines


# Function purpose: Translate the storage classification result into readable
#   dashboard copy.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as predicted_status, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _risk_summary_line(predicted_status: TrajectoryClassificationResult | None) -> str:
    """Translate the storage classification result into readable dashboard copy."""
    if predicted_status is None:
        return "No forecast risk classification was available for this horizon."
    prefix = "Worst forecast condition reaches" if predicted_status.status.level >= 2 else "Worst forecast condition stays at"
    return (
        f"{prefix} {predicted_status.status.short_label} at +{predicted_status.horizon_minute} min "
        f"({predicted_status.temp_c:.2f} C / {predicted_status.rh_pct:.2f}% RH)."
    )


# Function purpose: Explain, in one sentence, where the displayed scenario came
#   from.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as source, neighbor_count, case_count, interpreted
#   according to the implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _model_summary_line(*, source: str, neighbor_count: int, case_count: int) -> str:
    """Explain, in one sentence, where the displayed scenario came from."""
    source_label = str(source or "unknown").replace("_", " ")
    if neighbor_count > 0:
        return f"Forecast source: {source_label}, using {neighbor_count} nearest historical neighbours from a {case_count}-case analogue pool."
    if case_count > 0:
        return f"Forecast source: {source_label}, using a stored case pool of {case_count} trajectories."
    return f"Forecast source: {source_label}, without analogue neighbours for this horizon."


# Function purpose: Turn an internal event label into a phrase suitable for UI text.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _humanize_event(value: str) -> str:
    """Turn an internal event label into a phrase suitable for UI text."""
    text = str(value or "").strip().replace("_", " ")
    if text.lower() in {"", "none", "unknown", "event persist"}:
        return "live conditions"
    return text


# Function purpose: Recover the dew-point anchor from stored features, or recompute
#   it if missing.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as features, temp_start_c, rh_start_pct, interpreted
#   according to the implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _dew_point_anchor(*, features: dict[str, float], temp_start_c: float, rh_start_pct: float) -> float:
    """Recover the dew-point anchor from stored features, or recompute it if missing."""
    dew_last = features.get("dew_last")
    if dew_last is not None:
        return float(dew_last)
    return _dew_point_c(temp_start_c, rh_start_pct)


# Function purpose: Local dew-point helper kept in sync with the forecasting package
#   for UI fallback paths.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as temp_c, rh_pct, interpreted according to the
#   implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _dew_point_c(temp_c: float, rh_pct: float) -> float:
    """Local dew-point helper kept in sync with the forecasting package for UI fallback paths."""
    rh = max(1e-6, min(float(rh_pct), 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * float(temp_c) / (b + float(temp_c))) + math.log(rh)
    return (b * gamma) / (a - gamma)


# Function purpose: Return shared Plotly interaction settings used by forecast
#   charts.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Design reason: Service-layer code keeps presentation decisions separate from raw
#   data loading and lower-level runtime logic.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to the Flask routes and templates.

def _plotly_chart_config() -> dict[str, object]:
    """Return shared Plotly interaction settings used by forecast charts."""
    return {
        "displayModeBar": True,
        "displaylogo": False,
        "responsive": True,
        "scrollZoom": True,
        "doubleClick": "reset",
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    }
