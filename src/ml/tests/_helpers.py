# File overview:
# - Responsibility: Provides regression coverage for helpers behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

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
# Function purpose: Implements the synthetic window step used by this subsystem.
# - Project role: Belongs to the test and regression coverage and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as length, start, temp_base, rh_base, temp_rate_per_min,
#   rh_rate_per_min, interpreted according to the rules encoded in the body below.
# - Outputs: Returns list[TimeSeriesPoint] when the function completes successfully.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

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
# Function purpose: Loads fixture points into the structure expected by downstream
#   code.
# - Project role: Belongs to the test and regression coverage and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as name, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[TimeSeriesPoint] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

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
