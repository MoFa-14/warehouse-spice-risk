"""Services for loading pod history and rendering Plotly charts.

This module powers the history plots on the pod detail page. It sits between
the dashboard's raw data-access layer and the HTML templates, converting stored
telemetry into human-readable time windows and gap-aware Plotly figures.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

from app.data_access.csv_reader import read_processed_samples, read_raw_samples
from app.data_access.file_finder import find_processed_pod_files, find_raw_pod_files
from app.data_access.sqlite_reader import read_raw_samples_sqlite, sqlite_db_exists
from app.services.telemetry_adjustments import (
    apply_calibration,
    apply_smoothing,
    load_adjustments,
    recompute_dew_point,
)
from app.timezone import parse_datetime_local_input, timezone_label, to_display_time


RANGE_OPTIONS = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}
EXPECTED_SAMPLE_INTERVAL = timedelta(minutes=1)
GAP_BRIDGE_THRESHOLD = timedelta(minutes=2)


@dataclass(frozen=True)
class TimeWindow:
    """Requested chart time window."""

    key: str
    label: str
    start: datetime
    end: datetime
    custom: bool


def resolve_time_window(
    range_key: str | None,
    start_text: str | None,
    end_text: str | None,
    *,
    display_timezone: tzinfo | None = None,
    reference_end: datetime | None = None,
) -> TimeWindow:
    """Resolve a user-requested chart window into concrete UTC bounds.

    The dashboard lets the user select preset windows such as one hour or seven
    days, but the plotting layer ultimately needs exact timestamps. This helper
    centralises that conversion and keeps the route layer simple.
    """
    now = (reference_end or datetime.now(timezone.utc)).astimezone(timezone.utc)
    resolved_display_timezone = display_timezone or timezone.utc
    normalized = (range_key or "24h").lower()

    if normalized == "custom" and start_text and end_text:
        start = _parse_datetime(start_text, resolved_display_timezone)
        end = _parse_datetime(end_text, resolved_display_timezone)
        if end <= start:
            end = start + timedelta(hours=1)
        return TimeWindow(key="custom", label="Custom", start=start, end=end, custom=True)

    delta = RANGE_OPTIONS.get(normalized, RANGE_OPTIONS["24h"])
    resolved_key = normalized if normalized in RANGE_OPTIONS else "24h"
    return TimeWindow(key=resolved_key, label=resolved_key.upper(), start=now - delta, end=now, custom=False)


def build_timeseries_context(
    data_root: Path,
    pod_id: str,
    window: TimeWindow,
    *,
    max_points: int,
    db_path: Path | None = None,
    display_timezone: tzinfo | None = None,
    adjustments_path: Path | None = None,
) -> dict[str, object]:
    """Build the chart payload used by the pod detail page.

    The function loads raw and processed telemetry, applies the dashboard's
    presentation adjustments, filters the requested time window, and returns
    Plotly HTML fragments for temperature, relative humidity, and dew point.
    """
    resolved_display_timezone = display_timezone or timezone.utc
    date_from = window.start.date()
    date_to = window.end.date()

    raw_frame = _load_raw_frame(Path(data_root), pod_id, date_from=date_from, date_to=date_to, db_path=db_path)
    processed_frame = read_processed_samples(
        find_processed_pod_files(Path(data_root), pod_id, date_from=date_from, date_to=date_to)
    )

    raw_frame = _filter_window(raw_frame, window)
    processed_frame = _filter_window(processed_frame, window)
    adjustments = load_adjustments(adjustments_path)
    raw_frame = _adjust_raw_frame(raw_frame, adjustments)
    processed_frame = _adjust_processed_frame(processed_frame, adjustments)

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
    dew_frame = _dewpoint_frame(raw_frame, processed_frame)

    return {
        "has_data": True,
        "plotly_js": get_plotlyjs(),
        "temp_chart": _build_metric_chart(
            temp_frame,
            title="Temperature vs Time",
            y_label="Temperature (C)",
            color="#d97706",
            max_points=max_points,
            display_timezone=resolved_display_timezone,
        ),
        "rh_chart": _build_metric_chart(
            rh_frame,
            title="Relative Humidity vs Time",
            y_label="Relative Humidity (%)",
            color="#0f766e",
            max_points=max_points,
            display_timezone=resolved_display_timezone,
        ),
        "dew_chart": _build_metric_chart(
            dew_frame,
            title="Dew Point vs Time",
            y_label="Dew Point (C)",
            color="#2563eb",
            max_points=max_points,
            display_timezone=resolved_display_timezone,
        ) if not dew_frame.empty else None,
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


def _dewpoint_frame(raw_frame: pd.DataFrame, processed_frame: pd.DataFrame) -> pd.DataFrame:
    if not raw_frame.empty and "dew_point_c" in raw_frame.columns and raw_frame["dew_point_c"].notna().any():
        return raw_frame[["ts_pc_utc", "dew_point_c"]].rename(columns={"dew_point_c": "value"}).dropna()
    if not processed_frame.empty and "dew_point_c" in processed_frame.columns:
        return processed_frame[["ts_pc_utc", "dew_point_c"]].rename(columns={"dew_point_c": "value"}).dropna()
    return pd.DataFrame(columns=["ts_pc_utc", "value"])


def _load_raw_frame(
    data_root: Path,
    pod_id: str,
    *,
    date_from,
    date_to,
    db_path: Path | None = None,
) -> pd.DataFrame:
    """Load the raw telemetry view used as the preferred history source."""
    if db_path is not None and sqlite_db_exists(db_path):
        return read_raw_samples_sqlite(db_path, pod_id=pod_id, date_from=date_from, date_to=date_to)
    return read_raw_samples(find_raw_pod_files(Path(data_root), pod_id, date_from=date_from, date_to=date_to))


def _filter_window(frame: pd.DataFrame, window: TimeWindow) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame[(frame["ts_pc_utc"] >= pd.Timestamp(window.start)) & (frame["ts_pc_utc"] <= pd.Timestamp(window.end))].copy()


def _adjust_raw_frame(frame: pd.DataFrame, adjustments) -> pd.DataFrame:
    """Apply dashboard-only calibration and smoothing to raw telemetry."""
    if frame.empty:
        return frame
    adjusted = apply_calibration(frame, temp_column="temp_c", rh_column="rh_pct", adjustments=adjustments)
    adjusted = apply_smoothing(
        adjusted,
        value_columns=("temp_c", "rh_pct"),
        settings=adjustments.dashboard_smoothing,
    )
    return recompute_dew_point(adjusted, temp_column="temp_c", rh_column="rh_pct")


def _adjust_processed_frame(frame: pd.DataFrame, adjustments) -> pd.DataFrame:
    if frame.empty:
        return frame
    adjusted = apply_calibration(frame, temp_column="temp_c_clean", rh_column="rh_pct_clean", adjustments=adjustments)
    adjusted = apply_smoothing(
        adjusted,
        value_columns=("temp_c_clean", "rh_pct_clean"),
        settings=adjustments.dashboard_smoothing,
    )
    return recompute_dew_point(adjusted, temp_column="temp_c_clean", rh_column="rh_pct_clean")


def _downsample(frame: pd.DataFrame, max_points: int) -> pd.DataFrame:
    if frame.empty or len(frame) <= max_points:
        return frame
    step = max(1, len(frame) // max_points)
    return frame.iloc[::step].copy()


def _downsample_segments(segments: list[pd.DataFrame], *, max_points: int) -> list[pd.DataFrame]:
    total_points = sum(len(segment) for segment in segments)
    if total_points <= max_points:
        return [segment.copy() for segment in segments]
    step = max(1, total_points // max_points)
    sampled_segments: list[pd.DataFrame] = []
    for segment in segments:
        sampled = segment.iloc[::step].copy()
        if sampled.empty or sampled.iloc[-1]["ts_pc_utc"] != segment.iloc[-1]["ts_pc_utc"]:
            sampled = pd.concat([sampled, segment.iloc[[-1]].copy()])
        sampled_segments.append(sampled.reset_index(drop=True))
    return sampled_segments


def _build_metric_chart(
    frame: pd.DataFrame,
    *,
    title: str,
    y_label: str,
    color: str,
    max_points: int,
    display_timezone: tzinfo,
) -> str | None:
    figure = _build_metric_figure(
        frame,
        title=title,
        y_label=y_label,
        color=color,
        max_points=max_points,
        display_timezone=display_timezone,
    )
    if figure is None:
        return None
    return figure.to_html(full_html=False, include_plotlyjs=False, config=_plotly_chart_config())


def _build_metric_figure(
    frame: pd.DataFrame,
    *,
    title: str,
    y_label: str,
    color: str,
    max_points: int,
    display_timezone: tzinfo,
) -> go.Figure | None:
    """Build one gap-aware Plotly figure for a single metric.

    The key design choice here is that real data segments and no-reading gaps
    are represented separately. Solid segments indicate observed data. Dashed
    bridge lines indicate that time passed between two known observations but no
    samples were stored for that interval.
    """
    if frame.empty:
        return None
    local_frame = frame.copy().sort_values("ts_pc_utc").drop_duplicates(subset=["ts_pc_utc"], keep="last")
    segments = _split_frame_on_gaps(local_frame)
    sampled_segments = _downsample_segments(segments, max_points=max_points)
    observed_x, observed_y, gap_x, gap_y = _build_chart_series(sampled_segments, segments, display_timezone)
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=observed_x,
            y=observed_y,
            mode="lines+markers",
            name="Observed",
            connectgaps=False,
            line={"color": color, "width": 2.6},
            marker={"size": 5, "color": color, "line": {"width": 0}},
            fill="tozeroy",
            fillcolor=_alpha(color, 0.12),
            hovertemplate="%{x}<br>%{y:.2f}<extra>Observed</extra>",
            showlegend=bool(gap_x),
        )
    )
    if gap_x:
        figure.add_trace(
            go.Scatter(
                x=gap_x,
                y=gap_y,
                mode="lines",
                name="No readings",
                line={"color": _alpha(color, 0.7), "width": 2, "dash": "dash"},
                hovertemplate="No readings captured in this interval<extra></extra>",
            )
        )
    figure.update_layout(
        title={"text": title, "x": 0.02},
        template="plotly_white",
        margin={"l": 52, "r": 18, "t": 58, "b": 48},
        height=360,
        xaxis_title=f"Time ({timezone_label(display_timezone)})",
        yaxis_title=y_label,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fffdf8",
        font={"family": "Aptos, Trebuchet MS, sans-serif", "color": "#2b2216"},
        hovermode="x unified",
        dragmode="zoom",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        hoverlabel={"bgcolor": "#fffaf0", "bordercolor": "#d9c9aa", "font": {"color": "#2b2216"}},
    )
    figure.update_xaxes(
        showgrid=True,
        gridcolor="#e9e2d4",
        zeroline=False,
        automargin=True,
        showspikes=True,
        spikemode="across",
        spikecolor="#cbb38b",
        spikethickness=1,
    )
    figure.update_yaxes(showgrid=True, gridcolor="#e9e2d4", zeroline=False, automargin=True)
    return figure


def _build_chart_series(
    sampled_segments: list[pd.DataFrame],
    original_segments: list[pd.DataFrame],
    display_timezone: tzinfo,
) -> tuple[list[datetime | None], list[float | None], list[datetime | None], list[float | None]]:
    observed_x: list[datetime | None] = []
    observed_y: list[float | None] = []
    gap_x: list[datetime | None] = []
    gap_y: list[float | None] = []
    for index, sampled_segment in enumerate(sampled_segments):
        if index:
            observed_x.append(None)
            observed_y.append(None)
            previous_segment = original_segments[index - 1].iloc[-1]
            current_segment = original_segments[index].iloc[0]
            gap_x.extend(
                [
                    to_display_time(previous_segment["ts_pc_utc"].to_pydatetime(), display_timezone),
                    to_display_time(current_segment["ts_pc_utc"].to_pydatetime(), display_timezone),
                    None,
                ]
            )
            gap_y.extend([float(previous_segment["value"]), float(current_segment["value"]), None])
        observed_x.extend(
            to_display_time(value.to_pydatetime(), display_timezone)
            for value in sampled_segment["ts_pc_utc"]
        )
        observed_y.extend(float(value) for value in sampled_segment["value"])
    return observed_x, observed_y, gap_x, gap_y


def _split_frame_on_gaps(frame: pd.DataFrame) -> list[pd.DataFrame]:
    if frame.empty:
        return []
    segments: list[pd.DataFrame] = []
    start_index = 0
    for index in range(1, len(frame)):
        previous_ts = frame.iloc[index - 1]["ts_pc_utc"]
        current_ts = frame.iloc[index]["ts_pc_utc"]
        if _is_gap(previous_ts, current_ts):
            segments.append(frame.iloc[start_index:index].copy())
            start_index = index
    segments.append(frame.iloc[start_index:].copy())
    return segments


def _is_gap(previous_ts: pd.Timestamp, current_ts: pd.Timestamp) -> bool:
    return current_ts.to_pydatetime() - previous_ts.to_pydatetime() >= _gap_threshold()


def _gap_threshold() -> timedelta:
    # Pods publish roughly once per minute; treat 2+ minute jumps as missing-data gaps.
    return max(GAP_BRIDGE_THRESHOLD, EXPECTED_SAMPLE_INTERVAL * 2)


def _plotly_chart_config() -> dict[str, object]:
    return {
        "displayModeBar": True,
        "displaylogo": False,
        "responsive": True,
        "scrollZoom": True,
        "doubleClick": "reset",
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    }


def _parse_datetime(value: str, display_timezone: tzinfo) -> datetime:
    return parse_datetime_local_input(value, display_timezone)


def _alpha(hex_color: str, opacity: float) -> str:
    value = hex_color.lstrip("#")
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {opacity})"
