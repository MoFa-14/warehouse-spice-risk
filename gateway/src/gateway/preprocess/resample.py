"""Uniform-grid resampling for per-pod daily processed datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Sequence

from gateway.preprocess.clean import CleanSampleRow
from gateway.preprocess.dewpoint import dew_point_c


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


def _day_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _bucket_start(moment: datetime, *, day_start: datetime, interval_s: int) -> datetime:
    seconds_from_start = int((moment - day_start).total_seconds())
    offset = (seconds_from_start // interval_s) * interval_s
    return day_start + timedelta(seconds=offset)


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


def _lerp(start: float | None, end: float | None, ratio: float) -> float | None:
    if start is None or end is None:
        return None
    return start + ((end - start) * ratio)
