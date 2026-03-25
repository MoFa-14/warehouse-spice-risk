"""Services for loading pod history and rendering Plotly charts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

from app.data_access.csv_reader import read_processed_samples, read_raw_samples
from app.data_access.file_finder import find_processed_pod_files, find_raw_pod_files


RANGE_OPTIONS = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}


@dataclass(frozen=True)
class TimeWindow:
    """Requested chart time window."""

    key: str
    label: str
    start: datetime
    end: datetime
    custom: bool


def resolve_time_window(range_key: str | None, start_text: str | None, end_text: str | None) -> TimeWindow:
    """Resolve preset or custom time range parameters."""
    now = datetime.now(timezone.utc)
    normalized = (range_key or "24h").lower()

    if normalized == "custom" and start_text and end_text:
        start = _parse_datetime(start_text)
        end = _parse_datetime(end_text)
        if end <= start:
            end = start + timedelta(hours=1)
        return TimeWindow(key="custom", label="Custom", start=start, end=end, custom=True)

    delta = RANGE_OPTIONS.get(normalized, RANGE_OPTIONS["24h"])
    resolved_key = normalized if normalized in RANGE_OPTIONS else "24h"
    return TimeWindow(key=resolved_key, label=resolved_key.upper(), start=now - delta, end=now, custom=False)


def build_timeseries_context(data_root: Path, pod_id: str, window: TimeWindow, *, max_points: int) -> dict[str, object]:
    """Load time-series data and render Plotly charts for a pod."""
    date_from = window.start.date()
    date_to = window.end.date()

    raw_frame = read_raw_samples(find_raw_pod_files(Path(data_root), pod_id, date_from=date_from, date_to=date_to))
    processed_frame = read_processed_samples(
        find_processed_pod_files(Path(data_root), pod_id, date_from=date_from, date_to=date_to)
    )

    raw_frame = _filter_window(raw_frame, window)
    processed_frame = _filter_window(processed_frame, window)

    if raw_frame.empty and processed_frame.empty:
        return {
            "has_data": False,
            "plotly_js": "",
            "temp_chart": None,
            "rh_chart": None,
            "dew_chart": None,
            "window": window,
        }

    temp_frame = _temperature_frame(raw_frame, processed_frame)
    rh_frame = _humidity_frame(raw_frame, processed_frame)
    dew_frame = _dewpoint_frame(processed_frame)

    return {
        "has_data": True,
        "plotly_js": get_plotlyjs(),
        "temp_chart": _build_metric_chart(_downsample(temp_frame, max_points), title="Temperature vs Time", y_label="Temperature (C)", color="#d97706"),
        "rh_chart": _build_metric_chart(_downsample(rh_frame, max_points), title="Relative Humidity vs Time", y_label="Relative Humidity (%)", color="#0f766e"),
        "dew_chart": _build_metric_chart(_downsample(dew_frame, max_points), title="Dew Point vs Time", y_label="Dew Point (C)", color="#2563eb") if not dew_frame.empty else None,
        "window": window,
    }


def _temperature_frame(raw_frame: pd.DataFrame, processed_frame: pd.DataFrame) -> pd.DataFrame:
    if not raw_frame.empty and raw_frame["temp_c"].notna().any():
        return raw_frame[["ts_pc_utc", "temp_c"]].rename(columns={"temp_c": "value"}).dropna()
    if not processed_frame.empty:
        return processed_frame[["ts_pc_utc", "temp_c_clean"]].rename(columns={"temp_c_clean": "value"}).dropna()
    return pd.DataFrame(columns=["ts_pc_utc", "value"])


def _humidity_frame(raw_frame: pd.DataFrame, processed_frame: pd.DataFrame) -> pd.DataFrame:
    if not raw_frame.empty and raw_frame["rh_pct"].notna().any():
        return raw_frame[["ts_pc_utc", "rh_pct"]].rename(columns={"rh_pct": "value"}).dropna()
    if not processed_frame.empty:
        return processed_frame[["ts_pc_utc", "rh_pct_clean"]].rename(columns={"rh_pct_clean": "value"}).dropna()
    return pd.DataFrame(columns=["ts_pc_utc", "value"])


def _dewpoint_frame(processed_frame: pd.DataFrame) -> pd.DataFrame:
    if processed_frame.empty or "dew_point_c" not in processed_frame:
        return pd.DataFrame(columns=["ts_pc_utc", "value"])
    return processed_frame[["ts_pc_utc", "dew_point_c"]].rename(columns={"dew_point_c": "value"}).dropna()


def _filter_window(frame: pd.DataFrame, window: TimeWindow) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame[(frame["ts_pc_utc"] >= pd.Timestamp(window.start)) & (frame["ts_pc_utc"] <= pd.Timestamp(window.end))].copy()


def _downsample(frame: pd.DataFrame, max_points: int) -> pd.DataFrame:
    if frame.empty or len(frame) <= max_points:
        return frame
    step = max(1, len(frame) // max_points)
    return frame.iloc[::step].copy()


def _build_metric_chart(frame: pd.DataFrame, *, title: str, y_label: str, color: str) -> str | None:
    if frame.empty:
        return None
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["ts_pc_utc"],
            y=frame["value"],
            mode="lines",
            line={"color": color, "width": 2},
            fill="tozeroy",
            fillcolor=_alpha(color, 0.12),
            hovertemplate="%{x}<br>%{y:.2f}<extra></extra>",
        )
    )
    figure.update_layout(
        title=title,
        template="plotly_white",
        margin={"l": 36, "r": 12, "t": 48, "b": 36},
        height=320,
        xaxis_title="Time (UTC)",
        yaxis_title=y_label,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
    )
    figure.update_xaxes(showgrid=True, gridcolor="#e9e2d4")
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4")
    return figure.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False, "responsive": True})


def _parse_datetime(value: str) -> datetime:
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _alpha(hex_color: str, opacity: float) -> str:
    value = hex_color.lstrip("#")
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {opacity})"
