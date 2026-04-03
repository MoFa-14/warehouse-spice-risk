"""Forecast-side telemetry calibration and optional smoothing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import TimeSeriesPoint
from forecasting.utils import mean


@dataclass(frozen=True)
class PodCalibration:
    """Per-pod calibration offsets for derived forecast inputs."""

    temp_offset_c: float = 0.0
    rh_offset_pct: float = 0.0


@dataclass(frozen=True)
class SmoothingSettings:
    """Optional forecast-input smoothing settings."""

    enabled: bool = False
    method: str = "rolling_mean"
    window: int = 3


@dataclass(frozen=True)
class TelemetryAdjustments:
    """Resolved adjustment settings for forecast input windows."""

    default: PodCalibration = PodCalibration()
    pods: dict[str, PodCalibration] | None = None
    forecast_smoothing: SmoothingSettings = SmoothingSettings()

    def calibration_for_pod(self, pod_id: str) -> PodCalibration:
        pod_overrides = self.pods or {}
        return pod_overrides.get(str(pod_id), self.default)


def load_adjustments(path: Path | str | None) -> TelemetryAdjustments:
    """Load optional telemetry adjustments from JSON."""
    if path is None:
        return TelemetryAdjustments()

    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return TelemetryAdjustments()

    payload = json.loads(file_path.read_text(encoding="utf-8"))
    return TelemetryAdjustments(
        default=_parse_calibration(payload.get("default")),
        pods={
            str(pod_id): _parse_calibration(config)
            for pod_id, config in dict(payload.get("pods") or {}).items()
        },
        forecast_smoothing=_parse_smoothing(payload.get("forecast_smoothing")),
    )


def apply_calibration_to_rows(rows: list[dict[str, object]], *, pod_id: str, adjustments: TelemetryAdjustments) -> list[dict[str, object]]:
    """Apply calibration offsets to raw forecast input rows."""
    calibration = adjustments.calibration_for_pod(pod_id)
    adjusted_rows: list[dict[str, object]] = []
    for row in rows:
        temp_c = _optional_float(row.get("temp_c"))
        rh_pct = _optional_float(row.get("rh_pct"))
        if temp_c is not None:
            temp_c += calibration.temp_offset_c
        if rh_pct is not None:
            rh_pct = max(0.0, min(100.0, rh_pct + calibration.rh_offset_pct))
        adjusted_rows.append(
            {
                **row,
                "temp_c": temp_c,
                "rh_pct": rh_pct,
                "dew_point_c": calculate_dew_point_c(temp_c, rh_pct) if temp_c is not None and rh_pct is not None else None,
            }
        )
    return adjusted_rows


def apply_smoothing_to_points(points: list[TimeSeriesPoint], settings: SmoothingSettings) -> list[TimeSeriesPoint]:
    """Apply optional rolling smoothing to one resampled forecast window."""
    if not points or not settings.enabled or settings.window <= 1:
        return list(points)
    if settings.method != "rolling_mean":
        raise ValueError(f"Unsupported smoothing method: {settings.method}")

    smoothed: list[TimeSeriesPoint] = []
    for index, point in enumerate(points):
        window_points = points[max(0, index - settings.window + 1) : index + 1]
        temp_c = mean([candidate.temp_c for candidate in window_points])
        rh_pct = mean([candidate.rh_pct for candidate in window_points])
        smoothed.append(
            TimeSeriesPoint(
                ts_utc=point.ts_utc,
                temp_c=temp_c,
                rh_pct=rh_pct,
                dew_point_c=calculate_dew_point_c(temp_c, rh_pct),
                observed=point.observed,
            )
        )
    return smoothed


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
    return SmoothingSettings(
        enabled=bool(payload.get("enabled", False)),
        method=str(payload.get("method") or "rolling_mean").strip().lower(),
        window=max(1, int(payload.get("window") or 3)),
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return float(text)
