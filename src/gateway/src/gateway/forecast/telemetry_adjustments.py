# File overview:
# - Responsibility: Forecast-side telemetry calibration and optional smoothing.
# - Project role: Connects stored telemetry to forecasting, persistence, evaluation,
#   and calibration behavior.
# - Main data or concerns: History windows, forecast bundles, evaluation rows, and
#   calibration metadata.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.
# - Why this matters: This layer defines the live forecast lifecycle that the rest
#   of the project interprets and stores.

"""Forecast-side telemetry calibration and optional smoothing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import TimeSeriesPoint
from forecasting.utils import mean
# Class purpose: Per-pod calibration offsets for derived forecast inputs.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

@dataclass(frozen=True)
class PodCalibration:
    """Per-pod calibration offsets for derived forecast inputs."""

    temp_offset_c: float = 0.0
    rh_offset_pct: float = 0.0
# Class purpose: Optional forecast-input smoothing settings.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

@dataclass(frozen=True)
class SmoothingSettings:
    """Optional forecast-input smoothing settings."""

    enabled: bool = False
    method: str = "rolling_mean"
    window: int = 3
# Class purpose: Resolved adjustment settings for forecast input windows.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

@dataclass(frozen=True)
class TelemetryAdjustments:
    """Resolved adjustment settings for forecast input windows."""

    default: PodCalibration = PodCalibration()
    pods: dict[str, PodCalibration] | None = None
    forecast_smoothing: SmoothingSettings = SmoothingSettings()
    # Method purpose: Implements the calibration for pod step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on TelemetryAdjustments.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns PodCalibration when the function completes
    #   successfully.
    # - Important decisions: This layer defines the live forecast lifecycle that
    #   the rest of the project interprets and stores.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def calibration_for_pod(self, pod_id: str) -> PodCalibration:
        pod_overrides = self.pods or {}
        return pod_overrides.get(str(pod_id), self.default)
# Function purpose: Load optional telemetry adjustments from JSON.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as path, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns TelemetryAdjustments when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

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
# Function purpose: Apply calibration offsets to raw forecast input rows.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as rows, pod_id, adjustments, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns list[dict[str, object]] when the function completes
#   successfully.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

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
# Function purpose: Apply optional rolling smoothing to one resampled forecast
#   window.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as points, settings, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns list[TimeSeriesPoint] when the function completes successfully.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

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
# Function purpose: Parses calibration into structured values.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as payload, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns PodCalibration when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _parse_calibration(payload: object) -> PodCalibration:
    if not isinstance(payload, dict):
        return PodCalibration()
    return PodCalibration(
        temp_offset_c=float(payload.get("temp_offset_c") or 0.0),
        rh_offset_pct=float(payload.get("rh_offset_pct") or 0.0),
    )
# Function purpose: Parses smoothing into structured values.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as payload, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns SmoothingSettings when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _parse_smoothing(payload: object) -> SmoothingSettings:
    if not isinstance(payload, dict):
        return SmoothingSettings()
    return SmoothingSettings(
        enabled=bool(payload.get("enabled", False)),
        method=str(payload.get("method") or "rolling_mean").strip().lower(),
        window=max(1, int(payload.get("window") or 3)),
    )
# Function purpose: Implements the optional float step used by this subsystem.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: This layer defines the live forecast lifecycle that the
#   rest of the project interprets and stores.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return float(text)
