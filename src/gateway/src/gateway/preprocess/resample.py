# File overview:
# - Responsibility: Uniform-grid resampling for per-pod daily processed datasets.
# - Project role: Cleans, resamples, derives, or exports telemetry into
#   analysis-ready forms.
# - Main data or concerns: Time-series points, derived psychrometric variables, and
#   resampled grids.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.
# - Why this matters: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.

"""Uniform-grid resampling for per-pod daily processed datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Sequence

from gateway.preprocess.clean import CleanSampleRow
from gateway.preprocess.dewpoint import dew_point_c
# Class purpose: One uniform-grid processed time-series row.
# - Project role: Belongs to the gateway preprocessing layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

@dataclass
class ProcessedRow:
    """One uniform-grid processed time-series row."""

    ts_pc_utc: datetime
    pod_id: str
    temp_c_clean: float | None
    rh_pct_clean: float | None
    dew_point_c: float | None
    missing: int
    interpolated: int
    source_seq: int | None
# Function purpose: Implements the day start step used by this subsystem.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as day, interpreted according to the rules encoded in the
#   body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _day_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)
# Function purpose: Implements the bucket start step used by this subsystem.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as moment, day_start, interval_s, interpreted according
#   to the rules encoded in the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _bucket_start(moment: datetime, *, day_start: datetime, interval_s: int) -> datetime:
    seconds_from_start = int((moment - day_start).total_seconds())
    offset = (seconds_from_start // interval_s) * interval_s
    return day_start + timedelta(seconds=offset)
# Function purpose: Resample one pod/day into a full-day uniform grid.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as rows, day, pod_id, interval_s, interpolate,
#   max_gap_minutes, interpreted according to the rules encoded in the body below.
# - Outputs: Returns list[ProcessedRow] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def resample_day(
    rows: Sequence[CleanSampleRow],
    *,
    day: date,
    pod_id: str,
    interval_s: int = 60,
    interpolate: bool = False,
    max_gap_minutes: int = 5,
) -> list[ProcessedRow]:
    """Resample one pod/day into a full-day uniform grid."""
    if interval_s <= 0:
        raise ValueError("interval_s must be greater than 0")

    day_start = _day_start(day)
    day_end = day_start + timedelta(days=1)
    last_by_bucket: dict[datetime, CleanSampleRow] = {}
    for row in rows:
        if row.ts_pc_utc < day_start or row.ts_pc_utc >= day_end:
            continue
        last_by_bucket[_bucket_start(row.ts_pc_utc, day_start=day_start, interval_s=interval_s)] = row

    processed: list[ProcessedRow] = []
    moment = day_start
    while moment < day_end:
        sample = last_by_bucket.get(moment)
        if sample is None:
            processed.append(
                ProcessedRow(
                    ts_pc_utc=moment,
                    pod_id=pod_id,
                    temp_c_clean=None,
                    rh_pct_clean=None,
                    dew_point_c=None,
                    missing=1,
                    interpolated=0,
                    source_seq=None,
                )
            )
        else:
            processed.append(
                ProcessedRow(
                    ts_pc_utc=moment,
                    pod_id=pod_id,
                    temp_c_clean=sample.temp_c_clean,
                    rh_pct_clean=sample.rh_pct_clean,
                    dew_point_c=None,
                    missing=0,
                    interpolated=0,
                    source_seq=sample.seq,
                )
            )
        moment += timedelta(seconds=interval_s)

    if interpolate:
        _interpolate_small_gaps(processed, interval_s=interval_s, max_gap_minutes=max_gap_minutes)

    for row in processed:
        row.dew_point_c = dew_point_c(row.temp_c_clean, row.rh_pct_clean)

    return processed
# Function purpose: Implements the interpolate small gaps step used by this
#   subsystem.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as rows, interval_s, max_gap_minutes, interpreted
#   according to the rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _interpolate_small_gaps(rows: list[ProcessedRow], *, interval_s: int, max_gap_minutes: int) -> None:
    max_gap_seconds = max_gap_minutes * 60
    known_indexes = [index for index, row in enumerate(rows) if row.missing == 0]
    if len(known_indexes) < 2:
        return

    for left_index, right_index in zip(known_indexes, known_indexes[1:]):
        if right_index - left_index <= 1:
            continue

        left_row = rows[left_index]
        right_row = rows[right_index]
        gap_seconds = int((right_row.ts_pc_utc - left_row.ts_pc_utc).total_seconds())
        if gap_seconds > max_gap_seconds:
            continue

        total_steps = right_index - left_index
        for index in range(left_index + 1, right_index):
            ratio = (index - left_index) / total_steps
            row = rows[index]
            temp_c = _lerp(left_row.temp_c_clean, right_row.temp_c_clean, ratio)
            rh_pct = _lerp(left_row.rh_pct_clean, right_row.rh_pct_clean, ratio)
            if temp_c is None and rh_pct is None:
                continue
            row.temp_c_clean = temp_c
            row.rh_pct_clean = rh_pct
            row.interpolated = 1
# Function purpose: Implements the lerp step used by this subsystem.
# - Project role: Belongs to the gateway preprocessing layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as start, end, ratio, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Forecasting and dashboard analysis both depend on
#   preprocessing rules staying reproducible.
# - Related flow: Consumes raw or normalized telemetry and passes transformed
#   outputs to forecasting or export steps.

def _lerp(start: float | None, end: float | None, ratio: float) -> float | None:
    if start is None or end is None:
        return None
    return start + ((end - start) * ratio)
