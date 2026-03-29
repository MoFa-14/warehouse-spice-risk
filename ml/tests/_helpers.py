from __future__ import annotations

import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ML_SRC = Path(__file__).resolve().parents[1] / "src"
if str(ML_SRC) not in sys.path:
    sys.path.insert(0, str(ML_SRC))

from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import TimeSeriesPoint


def synthetic_window(
    *,
    length: int = 180,
    start: datetime | None = None,
    temp_base: float = 20.0,
    rh_base: float = 45.0,
    temp_rate_per_min: float = 0.01,
    rh_rate_per_min: float = 0.02,
) -> list[TimeSeriesPoint]:
    start_time = start or datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
    points: list[TimeSeriesPoint] = []
    for index in range(length):
        temp_c = temp_base + temp_rate_per_min * index
        rh_pct = rh_base + rh_rate_per_min * index
        points.append(
            TimeSeriesPoint(
                ts_utc=start_time + timedelta(minutes=index),
                temp_c=temp_c,
                rh_pct=rh_pct,
                dew_point_c=calculate_dew_point_c(temp_c, rh_pct),
                observed=True,
            )
        )
    return points


def load_fixture_points(name: str = "pod01_3h.csv") -> list[TimeSeriesPoint]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / name
    points: list[TimeSeriesPoint] = []
    with fixture_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            temp_c = float(row["temp_c"])
            rh_pct = float(row["rh_pct"])
            dew_point_c = calculate_dew_point_c(temp_c, rh_pct)
            points.append(
                TimeSeriesPoint(
                    ts_utc=datetime.fromisoformat(row["ts_pc_utc"].replace("Z", "+00:00")),
                    temp_c=temp_c,
                    rh_pct=rh_pct,
                    dew_point_c=dew_point_c,
                    observed=True,
                )
            )
    return points
