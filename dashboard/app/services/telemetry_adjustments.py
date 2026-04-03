"""Dashboard-side telemetry calibration and optional chart smoothing."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class PodCalibration:
    """Per-pod calibration offsets applied on read paths only."""

    temp_offset_c: float = 0.0
    rh_offset_pct: float = 0.0


@dataclass(frozen=True)
class SmoothingSettings:
    """Optional rolling smoothing for chart-friendly series."""

    enabled: bool = False
    method: str = "rolling_mean"
    window: int = 3


@dataclass(frozen=True)
class TelemetryAdjustments:
    """Resolved dashboard adjustment settings."""

    default: PodCalibration = PodCalibration()
    pods: dict[str, PodCalibration] | None = None
    dashboard_smoothing: SmoothingSettings = SmoothingSettings()

    def calibration_for_pod(self, pod_id: str) -> PodCalibration:
        pod_overrides = self.pods or {}
        return pod_overrides.get(str(pod_id), self.default)


def load_adjustments(path: Path | str | None) -> TelemetryAdjustments:
    """Load optional telemetry adjustment settings from JSON."""
    if path is None:
        return TelemetryAdjustments()

    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return TelemetryAdjustments()

    payload = json.loads(file_path.read_text(encoding="utf-8"))
    default = _parse_calibration(payload.get("default"))
    pods = {
        str(pod_id): _parse_calibration(config)
        for pod_id, config in dict(payload.get("pods") or {}).items()
    }
    return TelemetryAdjustments(
        default=default,
        pods=pods,
        dashboard_smoothing=_parse_smoothing(payload.get("dashboard_smoothing")),
    )


def apply_calibration(
    frame: pd.DataFrame,
    *,
    temp_column: str,
    rh_column: str,
    dew_column: str = "dew_point_c",
    pod_column: str = "pod_id",
    adjustments: TelemetryAdjustments | None = None,
) -> pd.DataFrame:
    """Apply per-pod calibration without mutating persisted raw storage."""
    if frame.empty:
        return frame
    resolved = adjustments or TelemetryAdjustments()
    adjusted = frame.copy()

    for pod_id, pod_frame in adjusted.groupby(pod_column):
        calibration = resolved.calibration_for_pod(str(pod_id))
        pod_mask = adjusted[pod_column] == pod_id

        temp_values = pd.to_numeric(adjusted.loc[pod_mask, temp_column], errors="coerce")
        rh_values = pd.to_numeric(adjusted.loc[pod_mask, rh_column], errors="coerce")

        adjusted.loc[pod_mask, temp_column] = temp_values + calibration.temp_offset_c
        adjusted.loc[pod_mask, rh_column] = (rh_values + calibration.rh_offset_pct).clip(lower=0.0, upper=100.0)

    adjusted[dew_column] = adjusted.apply(
        lambda row: _dew_point_c(row.get(temp_column), row.get(rh_column)),
        axis=1,
    )
    return adjusted


def apply_smoothing(
    frame: pd.DataFrame,
    *,
    value_columns: tuple[str, ...],
    ts_column: str = "ts_pc_utc",
    pod_column: str = "pod_id",
    settings: SmoothingSettings | None = None,
) -> pd.DataFrame:
    """Apply optional rolling smoothing to dashboard chart series."""
    resolved = settings or SmoothingSettings()
    if frame.empty or not resolved.enabled or resolved.window <= 1:
        return frame

    if resolved.method != "rolling_mean":
        raise ValueError(f"Unsupported smoothing method: {resolved.method}")

    smoothed_groups: list[pd.DataFrame] = []
    for _, pod_frame in frame.sort_values([pod_column, ts_column], kind="mergesort").groupby(pod_column, sort=False):
        smoothed = pod_frame.copy()
        for column in value_columns:
            values = pd.to_numeric(smoothed[column], errors="coerce")
            smoothed[column] = values.rolling(window=resolved.window, min_periods=1).mean()
        smoothed_groups.append(smoothed)

    return pd.concat(smoothed_groups, ignore_index=True) if smoothed_groups else frame


def recompute_dew_point(
    frame: pd.DataFrame,
    *,
    temp_column: str,
    rh_column: str,
    dew_column: str = "dew_point_c",
) -> pd.DataFrame:
    """Recompute dew point from the active temperature and RH columns."""
    if frame.empty:
        return frame
    updated = frame.copy()
    updated[dew_column] = updated.apply(
        lambda row: _dew_point_c(row.get(temp_column), row.get(rh_column)),
        axis=1,
    )
    return updated


def _parse_calibration(payload: object) -> PodCalibration:
    if not isinstance(payload, dict):
        return PodCalibration()
    return PodCalibration(
        temp_offset_c=float(payload.get("temp_offset_c") or 0.0),
        rh_offset_pct=float(payload.get("rh_offset_pct") or 0.0),
    )


def _parse_smoothing(payload: object) -> SmoothingSettings:
    if not isinstance(payload, dict):
        return SmoothingSettings()
    window = int(payload.get("window") or 3)
    return SmoothingSettings(
        enabled=bool(payload.get("enabled", False)),
        method=str(payload.get("method") or "rolling_mean").strip().lower(),
        window=max(1, window),
    )


def _dew_point_c(temp_c, rh_pct):
    if pd.isna(temp_c) or pd.isna(rh_pct):
        return float("nan")
    rh = max(1e-6, min(float(rh_pct), 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * float(temp_c) / (b + float(temp_c))) + math.log(rh)
    return (b * gamma) / (a - gamma)
