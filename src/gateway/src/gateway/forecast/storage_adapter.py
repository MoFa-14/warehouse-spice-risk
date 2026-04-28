# File overview:
# - Responsibility: Telemetry window loading for the live forecasting pipeline.
# - Project role: Connects stored telemetry to forecasting, persistence, evaluation,
#   and calibration behavior.
# - Main data or concerns: History windows, forecast bundles, evaluation rows, and
#   calibration metadata.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

"""Telemetry window loading for the live forecasting pipeline.

Responsibilities:
- Reads raw telemetry from SQLite or canonical per-pod CSV storage.
- Applies configured calibration and optional forecast-only smoothing.
- Rebuilds the fixed 1-minute windows used by forecasting, evaluation, and
  historical case learning.

Project flow:
- raw stored samples -> calibration -> minute-grid resampling -> forecasting
  history window / realised future window

Why this matters:
- Every forecast, evaluation, and learned case depends on all windows sharing
  the same temporal structure and variable definitions.
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from gateway.forecast import _ensure_forecasting_package
from gateway.forecast.telemetry_adjustments import (
    apply_calibration_to_rows,
    apply_smoothing_to_points,
    load_adjustments,
)
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.sqlite_db import connect_sqlite, resolve_db_path
from gateway.storage.sqlite_reader import samples_in_range

_ensure_forecasting_package()

from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import TimeSeriesPoint
from forecasting.utils import floor_to_minute, minute_points, parse_utc


LOGGER = logging.getLogger(__name__)


# Class purpose: Resampled telemetry window and basic data-quality statistics.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

@dataclass(frozen=True)
class WindowResult:
    """Resampled telemetry window and basic data-quality statistics.

    The runner needs both the points themselves and a simple quality summary so
    it can decide whether a window is complete enough to trust.
    """

    points: list[TimeSeriesPoint]
    missing_rate: float


# Forecast telemetry adapter
# - Purpose: loads forecast-ready telemetry windows without exposing backend-
#   specific storage details to the runner.
# - Project role: data-access and preprocessing boundary between raw storage and
#   the forecasting package.
# - Inputs: preferred backend, storage paths, and optional calibration file.
# - Outputs: minute-grid history and future windows plus missing-rate metadata.
# Class purpose: Read per-pod telemetry from the preferred live storage backend.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

class ForecastStorageAdapter:
    """Read per-pod telemetry from the preferred live storage backend."""

    # Method purpose: Handles init for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as storage_backend, db_path, data_root,
    #   adjustments_path, interpreted according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def __init__(
        self,
        *,
        storage_backend: str,
        db_path=None,
        data_root=None,
        adjustments_path=None,
    ) -> None:
        self.requested_backend = storage_backend.strip().lower()
        self.storage_paths = build_storage_paths(data_root)
        self.db_path = resolve_db_path(db_path)
        self.adjustments = load_adjustments(
            Path(adjustments_path)
            if adjustments_path is not None
            else self.storage_paths.root / "config" / "telemetry_adjustments.json"
        )
        self.storage_backend = self._resolve_backend()

    # Method purpose: Return all pod IDs currently visible in the chosen storage
    #   backend.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns list[str] when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def list_pod_ids(self) -> list[str]:
        """Return all pod IDs currently visible in the chosen storage backend."""
        if self.storage_backend == "sqlite":
            return self._list_pods_sqlite()
        pod_ids = [path.name for path in self.storage_paths.raw_pods_root.iterdir() if path.is_dir()]
        return sorted(pod_ids)

    # Method purpose: Return the most recent timestamp for one pod.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def latest_timestamp(self, pod_id: str) -> datetime | None:
        """Return the most recent timestamp for one pod."""
        if self.storage_backend == "sqlite":
            return self._latest_timestamp_sqlite(pod_id)
        return self._latest_timestamp_csv(pod_id)

    # Method purpose: Return the earliest timestamp for one pod.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def earliest_timestamp(self, pod_id: str) -> datetime | None:
        """Return the earliest timestamp for one pod."""
        if self.storage_backend == "sqlite":
            return self._earliest_timestamp_sqlite(pod_id)
        return self._earliest_timestamp_csv(pod_id)

    # Forecast timestamp resolution
    # - Purpose: determines the actual timestamp at which the next forecast is
    #   allowed to run.
    # - Important decision: requested times are floored to the minute grid and
    #   clamped so no forecast is issued beyond the newest available sample.
    # Method purpose: Choose the actual timestamp at which forecasting should
    #   occur.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, requested_time_utc, interpreted
    #   according to the implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def effective_forecast_time(self, *, pod_id: str, requested_time_utc: datetime | None = None) -> datetime | None:
        """Choose the actual timestamp at which forecasting should occur."""
        latest = self.latest_timestamp(pod_id)
        if latest is None:
            return None
        latest_minute = floor_to_minute(latest)
        if requested_time_utc is None:
            return latest_minute
        return min(floor_to_minute(requested_time_utc), latest_minute)

    # History-window load path
    # - Purpose: reconstructs the fixed-length history window used as forecast
    #   input.
    # - Project role: preprocessing stage ahead of event detection and feature
    #   extraction.
    # Method purpose: Load the 3-hour history window that becomes forecast
    #   input.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, as_of_utc, minutes, interpreted
    #   according to the implementation below.
    # - Outputs: Returns WindowResult when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def load_history_window(self, *, pod_id: str, as_of_utc: datetime, minutes: int) -> WindowResult:
        """Load the 3-hour history window that becomes forecast input."""
        end = floor_to_minute(as_of_utc)
        timestamps = minute_points(end, minutes)
        start = timestamps[0]
        rows = self._raw_rows(
            pod_id=pod_id,
            ts_from_utc=start - timedelta(minutes=5),
            ts_to_utc=end + timedelta(minutes=1),
        )
        return self._smooth_window(_resample_rows(rows=rows, timestamps=timestamps))

    # Realised-future load path
    # - Purpose: reconstructs the realised 30-minute future used for later
    #   forecast evaluation and case learning.
    # Method purpose: Load the 30-minute future window used later for
    #   evaluation.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, ts_forecast_utc, minutes, interpreted
    #   according to the implementation below.
    # - Outputs: Returns WindowResult when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def load_actual_horizon(self, *, pod_id: str, ts_forecast_utc: datetime, minutes: int) -> WindowResult:
        """Load the 30-minute future window used later for evaluation."""
        start = floor_to_minute(ts_forecast_utc) + timedelta(minutes=1)
        timestamps = [start + timedelta(minutes=index) for index in range(minutes)]
        rows = self._raw_rows(
            pod_id=pod_id,
            ts_from_utc=start - timedelta(minutes=1),
            ts_to_utc=timestamps[-1] + timedelta(minutes=1),
        )
        return self._smooth_window(_resample_rows(rows=rows, timestamps=timestamps))

    # Backend resolution
    # - Purpose: decides whether the live forecasting path should read from
    #   SQLite or fall back to canonical CSV files.
    # Method purpose: Pick the live source of truth available in the current
    #   environment.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns str when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _resolve_backend(self) -> str:
        """Pick the live source of truth available in the current environment."""
        if self.requested_backend == "csv":
            return "csv"
        if not self.db_path.exists():
            LOGGER.warning("SQLite database %s not found; falling back to CSV forecasting input.", self.db_path)
            return "csv"
        try:
            connection = connect_sqlite(self.db_path, readonly=True)
            try:
                connection.execute("SELECT 1 FROM samples_raw LIMIT 1").fetchone()
            finally:
                connection.close()
        except sqlite3.Error:
            LOGGER.warning("SQLite samples_raw table unavailable; falling back to CSV forecasting input.")
            return "csv"
        return "sqlite"

    # Method purpose: Lists pods SQLite for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns list[str] when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _list_pods_sqlite(self) -> list[str]:
        if not self.db_path.exists():
            return []
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            rows = connection.execute("SELECT DISTINCT pod_id FROM samples_raw ORDER BY pod_id ASC").fetchall()
        finally:
            connection.close()
        return [str(row["pod_id"]) for row in rows]

    # Method purpose: Retrieves the latest timestamp SQLite for the calling
    #   code.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _latest_timestamp_sqlite(self, pod_id: str) -> datetime | None:
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            row = connection.execute(
                "SELECT ts_pc_utc FROM samples_raw WHERE pod_id = ? ORDER BY ts_pc_utc DESC LIMIT 1",
                (str(pod_id),),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        return parse_utc(row["ts_pc_utc"])

    # Method purpose: Handles timestamp SQLite for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _earliest_timestamp_sqlite(self, pod_id: str) -> datetime | None:
        connection = connect_sqlite(self.db_path, readonly=True)
        try:
            row = connection.execute(
                "SELECT ts_pc_utc FROM samples_raw WHERE pod_id = ? ORDER BY ts_pc_utc ASC LIMIT 1",
                (str(pod_id),),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        return parse_utc(row["ts_pc_utc"])

    # Method purpose: Retrieves the latest timestamp CSV for the calling code.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _latest_timestamp_csv(self, pod_id: str) -> datetime | None:
        pod_dir = self.storage_paths.raw_pods_root / str(pod_id)
        if not pod_dir.exists():
            return None
        day_files = sorted(pod_dir.glob("*.csv"))
        if not day_files:
            return None
        latest: datetime | None = None
        for path in reversed(day_files[-2:]):
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    latest = parse_utc(row["ts_pc_utc"])
        return latest

    # Method purpose: Handles timestamp CSV for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, interpreted according to the
    #   implementation below.
    # - Outputs: Returns datetime | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _earliest_timestamp_csv(self, pod_id: str) -> datetime | None:
        pod_dir = self.storage_paths.raw_pods_root / str(pod_id)
        if not pod_dir.exists():
            return None
        day_files = sorted(pod_dir.glob("*.csv"))
        for path in day_files:
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    return parse_utc(row["ts_pc_utc"])
        return None

    # Raw-row load and calibration
    # - Purpose: reads one pod's raw rows for a time range and applies the
    #   configured calibration adjustments.
    # - Important decision: dew point is recomputed when needed so all later
    #   stages receive a complete temperature/RH/dew triplet.
    # Method purpose: Read raw rows for one pod and time range, then apply
    #   calibration.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as pod_id, ts_from_utc, ts_to_utc, interpreted
    #   according to the implementation below.
    # - Outputs: Returns list[dict[str, Any]] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _raw_rows(self, *, pod_id: str, ts_from_utc: datetime, ts_to_utc: datetime) -> list[dict[str, Any]]:
        """Read raw rows for one pod and time range, then apply calibration."""
        if self.storage_backend == "sqlite":
            rows = samples_in_range(
                db_path=self.db_path,
                pod_id=str(pod_id),
                ts_from_utc=_iso(ts_from_utc),
                ts_to_utc=_iso(ts_to_utc),
            )
            calibrated_rows = [
                {
                    "ts_pc_utc": row["ts_pc_utc"],
                    "temp_c": row.get("temp_c"),
                    "rh_pct": row.get("rh_pct"),
                    "dew_point_c": calculate_dew_point_c(float(row["temp_c"]), float(row["rh_pct"]))
                    if row.get("temp_c") is not None and row.get("rh_pct") is not None
                    else None,
                }
                for row in rows
            ]
            return apply_calibration_to_rows(calibrated_rows, pod_id=str(pod_id), adjustments=self.adjustments)

        return apply_calibration_to_rows(
            _read_csv_rows(
                storage_paths=self.storage_paths,
                pod_id=str(pod_id),
                ts_from_utc=ts_from_utc,
                ts_to_utc=ts_to_utc,
            ),
            pod_id=str(pod_id),
            adjustments=self.adjustments,
        )

    # Forecast-only smoothing
    # - Purpose: applies optional smoothing after resampling while keeping the
    #   stored raw telemetry unchanged.
    # Method purpose: Apply any configured forecast-only smoothing to the
    #   resampled window.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastStorageAdapter.
    # - Inputs: Arguments such as result, interpreted according to the
    #   implementation below.
    # - Outputs: Returns WindowResult when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _smooth_window(self, result: WindowResult) -> WindowResult:
        """Apply any configured forecast-only smoothing to the resampled window."""
        return WindowResult(
            points=apply_smoothing_to_points(result.points, self.adjustments.forecast_smoothing),
            missing_rate=result.missing_rate,
        )


# CSV raw-row loader
# - Purpose: loads raw telemetry rows from canonical per-pod CSV files for the
#   requested time range.
# Function purpose: Read raw CSV telemetry rows for one pod and time range.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as storage_paths, pod_id, ts_from_utc, ts_to_utc,
#   interpreted according to the implementation below.
# - Outputs: Returns list[dict[str, Any]] when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _read_csv_rows(
    *,
    storage_paths: StoragePaths,
    pod_id: str,
    ts_from_utc: datetime,
    ts_to_utc: datetime,
) -> list[dict[str, Any]]:
    """Read raw CSV telemetry rows for one pod and time range."""
    rows: list[dict[str, Any]] = []
    pod_dir = storage_paths.raw_pods_root / pod_id
    if not pod_dir.exists():
        return rows

    day = ts_from_utc.date()
    final_day = ts_to_utc.date()
    while day <= final_day:
        path = pod_dir / f"{day.isoformat()}.csv"
        if path.exists():
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    ts_value = parse_utc(row["ts_pc_utc"])
                    if ts_value < ts_from_utc or ts_value >= ts_to_utc:
                        continue
                    temp_c = _optional_float(row.get("temp_c"))
                    rh_pct = _optional_float(row.get("rh_pct"))
                    dew_point = _optional_float(row.get("dew_point_c"))
                    if dew_point is None and temp_c is not None and rh_pct is not None:
                        dew_point = calculate_dew_point_c(temp_c, rh_pct)
                    rows.append(
                        {
                            "ts_pc_utc": row["ts_pc_utc"],
                            "temp_c": temp_c,
                            "rh_pct": rh_pct,
                            "dew_point_c": dew_point,
                        }
                    )
        day += timedelta(days=1)
    return rows


# Minute-grid reconstruction
# - Purpose: converts irregular raw rows into the fixed 1-minute structure used
#   by forecasting.
# - Project role: common preprocessing stage for live forecasting, realised
#   future evaluation, and historical case learning.
# - Inputs: calibrated raw rows and the exact timestamps expected by the target
#   window.
# - Outputs: ``WindowResult`` with minute-aligned points and missing-rate
#   metadata.
# - Important decisions:
#   - average multiple samples within the same minute
#   - interpolate only across surrounding known values
#   - preserve an observed/reconstructed flag so completeness remains measurable
# Function purpose: Resample irregular/raw rows onto the fixed 1-minute forecasting
#   grid.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as rows, timestamps, interpreted according to the
#   implementation below.
# - Outputs: Returns WindowResult when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _resample_rows(*, rows: list[dict[str, Any]], timestamps: list[datetime]) -> WindowResult:
    """Resample irregular/raw rows onto the fixed 1-minute forecasting grid."""
    buckets: dict[datetime, dict[str, list[float]]] = defaultdict(lambda: {"temp": [], "rh": [], "dew": []})
    for row in rows:
        temp_c = row.get("temp_c")
        rh_pct = row.get("rh_pct")
        dew_point = row.get("dew_point_c")
        if temp_c is None or rh_pct is None:
            continue
        bucket_time = floor_to_minute(parse_utc(row["ts_pc_utc"]))
        bucket = buckets[bucket_time]
        bucket["temp"].append(float(temp_c))
        bucket["rh"].append(float(rh_pct))
        bucket["dew"].append(float(dew_point) if dew_point is not None else calculate_dew_point_c(float(temp_c), float(rh_pct)))

    # These aligned arrays become the canonical minute-by-minute forecasting
    # series after interpolation and observed-flag assignment.
    known_values: dict[int, tuple[float, float, float]] = {}
    observed_flags: list[bool] = []
    temp_series: list[float | None] = []
    rh_series: list[float | None] = []
    dew_series: list[float | None] = []
    for index, timestamp in enumerate(timestamps):
        bucket = buckets.get(timestamp)
        if bucket and bucket["temp"] and bucket["rh"]:
            temp_value = sum(bucket["temp"]) / float(len(bucket["temp"]))
            rh_value = sum(bucket["rh"]) / float(len(bucket["rh"]))
            dew_value = sum(bucket["dew"]) / float(len(bucket["dew"]))
            temp_series.append(temp_value)
            rh_series.append(rh_value)
            dew_series.append(dew_value)
            known_values[index] = (temp_value, rh_value, dew_value)
            observed_flags.append(True)
        else:
            temp_series.append(None)
            rh_series.append(None)
            dew_series.append(None)
            observed_flags.append(False)

    if not known_values:
        return WindowResult(points=[], missing_rate=1.0)

    for index in range(len(timestamps)):
        if temp_series[index] is not None:
            continue
        previous_index = max((candidate for candidate in known_values if candidate < index), default=None)
        next_index = min((candidate for candidate in known_values if candidate > index), default=None)
        if previous_index is None and next_index is None:
            continue
        # Missing minutes are reconstructed from neighbouring evidence so the
        # forecasting grid stays contiguous while the missing-rate still records
        # how much of the window was originally observed.
        if previous_index is None:
            temp_value, rh_value, dew_value = known_values[next_index]
        elif next_index is None:
            temp_value, rh_value, dew_value = known_values[previous_index]
        else:
            ratio = (index - previous_index) / float(next_index - previous_index)
            prev_temp, prev_rh, prev_dew = known_values[previous_index]
            next_temp, next_rh, next_dew = known_values[next_index]
            temp_value = prev_temp + (next_temp - prev_temp) * ratio
            rh_value = prev_rh + (next_rh - prev_rh) * ratio
            dew_value = prev_dew + (next_dew - prev_dew) * ratio
        temp_series[index] = temp_value
        rh_series[index] = rh_value
        dew_series[index] = dew_value

    points = [
        TimeSeriesPoint(
            ts_utc=timestamp,
            temp_c=float(temp_series[index]),
            rh_pct=float(rh_series[index]),
            dew_point_c=float(dew_series[index]),
            observed=observed_flags[index],
        )
        for index, timestamp in enumerate(timestamps)
        if temp_series[index] is not None and rh_series[index] is not None and dew_series[index] is not None
    ]
    missing_rate = 1.0 - (sum(1 for flag in observed_flags if flag) / float(len(observed_flags)))
    return WindowResult(points=points, missing_rate=missing_rate)


# Optional float coercion
# - Purpose: converts raw storage text fields into floats while accepting common
#   missing-value markers.
# Function purpose: Convert storage text fields into floats while tolerating empty
#   markers.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns float | None when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _optional_float(value: object) -> float | None:
    """Convert storage text fields into floats while tolerating empty markers."""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return float(text)


# UTC serialisation helper
# - Purpose: keeps forecast storage queries aligned with the repository's stable
#   UTC timestamp format.
# Function purpose: Serialise a UTC datetime in the repository's stable format.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the implementation
#   below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _iso(value: datetime) -> str:
    """Serialise a UTC datetime in the repository's stable format."""
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
