"""Dashboard services for rendering stored forecasts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from datetime import tzinfo

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

from app.data_access.forecast_reader import read_latest_forecasts
from app.services.thresholds import TrajectoryClassificationResult, classify_storage_trajectory
from app.timezone import timezone_label, to_display_time


@dataclass(frozen=True)
class PredictionEvaluationView:
    mae_temp_c: float
    rmse_temp_c: float
    mae_rh_pct: float
    rmse_rh_pct: float
    large_error: bool
    notes: str


@dataclass(frozen=True)
class PredictionScenarioView:
    scenario: str
    source: str
    neighbor_count: int
    case_count: int
    temp_start_c: float
    temp_end_c: float
    rh_start_pct: float
    rh_end_pct: float
    dew_end_c: float
    notes: str
    temp_forecast_c: list[float]
    rh_forecast_pct: list[float]
    temp_p25_c: list[float]
    temp_p75_c: list[float]
    rh_p25_pct: list[float]
    rh_p75_pct: list[float]
    predicted_status: TrajectoryClassificationResult | None
    evaluation: PredictionEvaluationView | None


@dataclass(frozen=True)
class PodPredictionView:
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


def build_prediction_page_context(
    data_root: Path,
    *,
    db_path: Path | None = None,
    selected_pod_id: str | None = None,
    display_timezone: tzinfo | None = None,
) -> dict[str, object]:
    """Build the prediction page view with summary cards and full forecast panels per pod."""
    frame = read_latest_forecasts(Path(data_root), db_path=db_path)
    resolved_display_timezone = display_timezone or timezone.utc
    summary_predictions = _build_predictions(frame, include_charts_for=set(), display_timezone=resolved_display_timezone)
    selected = None
    predictions: list[PodPredictionView] = []
    if summary_predictions:
        selected_id = str(selected_pod_id or summary_predictions[0].pod_id)
        if all(item.pod_id != selected_id for item in summary_predictions):
            selected_id = summary_predictions[0].pod_id
        predictions = _build_predictions(
            frame,
            include_charts_for={item.pod_id for item in summary_predictions},
            display_timezone=resolved_display_timezone,
        )
        predictions = _prioritize_predictions(predictions, selected_id)
        selected = next((item for item in predictions if item.pod_id == selected_id), predictions[0])
    return {
        "has_predictions": bool(predictions),
        "predictions": predictions,
        "selected_prediction": selected,
        "plotly_js": get_plotlyjs() if predictions else "",
    }


def build_pod_prediction_context(
    data_root: Path,
    pod_id: str,
    *,
    db_path: Path | None = None,
    display_timezone: tzinfo | None = None,
) -> PodPredictionView | None:
    """Build the latest available forecast view for one pod detail page."""
    frame = read_latest_forecasts(Path(data_root), db_path=db_path, pod_id=pod_id)
    predictions = _build_predictions(frame, include_charts_for={pod_id}, display_timezone=display_timezone or timezone.utc)
    return predictions[0] if predictions else None


def _build_predictions(
    frame: pd.DataFrame,
    *,
    include_charts_for: set[str],
    display_timezone: tzinfo,
) -> list[PodPredictionView]:
    if frame.empty:
        return []

    chart_pods = {str(pod_id) for pod_id in include_charts_for}
    views: list[PodPredictionView] = []
    for pod_id, pod_frame in frame.groupby("pod_id", sort=True):
        baseline_row = _scenario_row(pod_frame, "baseline")
        if baseline_row is None:
            continue
        event_row = _scenario_row(pod_frame, "event_persist")
        baseline = _scenario_view(baseline_row)
        event_persist = _scenario_view(event_row) if event_row is not None else None
        temp_chart = None
        rh_chart = None
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
                plotly_js=get_plotlyjs() if temp_chart is not None or rh_chart is not None else "",
                temp_chart=temp_chart,
                rh_chart=rh_chart,
            )
        )
    return views


def _prioritize_predictions(predictions: list[PodPredictionView], selected_pod_id: str) -> list[PodPredictionView]:
    selected = [item for item in predictions if item.pod_id == selected_pod_id]
    remaining = [item for item in predictions if item.pod_id != selected_pod_id]
    return selected + remaining


def _scenario_row(frame: pd.DataFrame, scenario: str):
    rows = frame[frame["scenario"] == scenario]
    if rows.empty:
        return None
    return rows.iloc[0]


def _scenario_view(row) -> PredictionScenarioView:
    if row is None:
        raise ValueError("Cannot build scenario view from an empty row.")
    forecast = json.loads(row["json_forecast"])
    p25 = json.loads(row["json_p25"])
    p75 = json.loads(row["json_p75"])
    temp_forecast_c = [float(value) for value in forecast["temp_forecast_c"]]
    rh_forecast_pct = [float(value) for value in forecast["rh_forecast_pct"]]
    predicted_status = classify_storage_trajectory(temp_forecast_c, rh_forecast_pct)
    features = forecast.get("feature_vector") or {}
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
        dew_end_c=float((forecast.get("dew_point_forecast_c") or [0.0])[-1]),
        notes=str(forecast.get("notes") or ""),
        temp_forecast_c=temp_forecast_c,
        rh_forecast_pct=rh_forecast_pct,
        temp_p25_c=[float(value) for value in p25["temp_c"]],
        temp_p75_c=[float(value) for value in p75["temp_c"]],
        rh_p25_pct=[float(value) for value in p25["rh_pct"]],
        rh_p75_pct=[float(value) for value in p75["rh_pct"]],
        predicted_status=predicted_status,
        evaluation=evaluation,
    )


def _build_forecast_chart(
    *,
    ts_pc_utc: datetime,
    baseline: PredictionScenarioView,
    alternate: PredictionScenarioView | None,
    metric: str,
    display_timezone: tzinfo,
) -> str:
    times = [to_display_time(ts_pc_utc + timedelta(minutes=index), display_timezone) for index in range(31)]
    figure = go.Figure()

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
    else:
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
    return figure.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False, "responsive": True})


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


def _alpha(hex_color: str, opacity: float) -> str:
    value = hex_color.lstrip("#")
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {opacity})"
