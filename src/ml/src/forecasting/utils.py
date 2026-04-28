# File overview:
# - Responsibility: Small numerical and time helpers for forecasting.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Small numerical and time helpers for forecasting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import floor, sqrt
from statistics import median
from typing import Iterable, Sequence


UTC = timezone.utc
# Function purpose: Parse an ISO-8601 UTC timestamp into an aware datetime.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def parse_utc(value: str | datetime) -> datetime:
    """Parse an ISO-8601 UTC timestamp into an aware datetime."""
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
# Function purpose: Serialize a UTC datetime as the repository's stable ISO string.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def to_utc_iso(value: datetime) -> str:
    """Serialize a UTC datetime as the repository's stable ISO string."""
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
# Function purpose: Drop seconds and microseconds from a UTC timestamp.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def floor_to_minute(value: datetime) -> datetime:
    """Drop seconds and microseconds from a UTC timestamp."""
    normalized = value.astimezone(UTC)
    return normalized.replace(second=0, microsecond=0)
# Function purpose: Floor a timestamp to the most recent N-minute boundary.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, minutes, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def floor_to_interval(value: datetime, minutes: int) -> datetime:
    """Floor a timestamp to the most recent N-minute boundary."""
    normalized = floor_to_minute(value)
    total_minutes = normalized.hour * 60 + normalized.minute
    floored_minutes = (total_minutes // minutes) * minutes
    return normalized.replace(hour=0, minute=0) + timedelta(minutes=floored_minutes)
# Function purpose: Return count UTC timestamps ending at end with 1-minute spacing.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as end, count, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns list[datetime] when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def minute_points(end: datetime, count: int) -> list[datetime]:
    """Return count UTC timestamps ending at end with 1-minute spacing."""
    aligned_end = floor_to_minute(end)
    start = aligned_end - timedelta(minutes=count - 1)
    return [start + timedelta(minutes=index) for index in range(count)]
# Function purpose: Clamp a value to an inclusive range.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, lower, upper, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def clamp(value: float, lower: float, upper: float) -> float:
    """Clamp a value to an inclusive range."""
    return max(lower, min(upper, value))
# Function purpose: Return a simple linear-interpolated percentile.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, q, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def percentile(values: Sequence[float], q: float) -> float:
    """Return a simple linear-interpolated percentile."""
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(value) for value in values)
    rank = (len(ordered) - 1) * (q / 100.0)
    lower_index = floor(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = rank - lower_index
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight
# Function purpose: Return the arithmetic mean, defaulting to zero for empty input.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def mean(values: Sequence[float]) -> float:
    """Return the arithmetic mean, defaulting to zero for empty input."""
    if not values:
        return 0.0
    return sum(float(value) for value in values) / float(len(values))
# Function purpose: Return the population standard deviation.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def population_std(values: Sequence[float]) -> float:
    """Return the population standard deviation."""
    if len(values) < 2:
        return 0.0
    average = mean(values)
    variance = sum((float(value) - average) ** 2 for value in values) / float(len(values))
    return sqrt(variance)
# Function purpose: Return the median absolute deviation.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def median_absolute_deviation(values: Sequence[float]) -> float:
    """Return the median absolute deviation."""
    if not values:
        return 0.0
    center = median(float(value) for value in values)
    return median(abs(float(value) - center) for value in values)
# Function purpose: Return the per-step least-squares slope of a sequence.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def linear_regression_slope(values: Sequence[float]) -> float:
    """Return the per-step least-squares slope of a sequence."""
    if len(values) < 2:
        return 0.0
    x_mean = (len(values) - 1) / 2.0
    y_mean = mean(values)
    numerator = 0.0
    denominator = 0.0
    for index, value in enumerate(values):
        dx = index - x_mean
        numerator += dx * (float(value) - y_mean)
        denominator += dx * dx
    if denominator == 0.0:
        return 0.0
    return numerator / denominator
# Function purpose: Return value[t] - value[t-gap] differences.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, gap, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns list[float] when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def pairwise_differences(values: Sequence[float], gap: int = 1) -> list[float]:
    """Return value[t] - value[t-gap] differences."""
    if len(values) <= gap:
        return []
    return [float(values[index]) - float(values[index - gap]) for index in range(gap, len(values))]
# Function purpose: Return mean absolute error.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as predicted, actual, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def mae(predicted: Sequence[float], actual: Sequence[float]) -> float:
    """Return mean absolute error."""
    if not predicted or not actual:
        return 0.0
    count = min(len(predicted), len(actual))
    return sum(abs(float(predicted[index]) - float(actual[index])) for index in range(count)) / float(count)
# Function purpose: Return root mean square error.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as predicted, actual, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns float when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def rmse(predicted: Sequence[float], actual: Sequence[float]) -> float:
    """Return root mean square error."""
    if not predicted or not actual:
        return 0.0
    count = min(len(predicted), len(actual))
    return sqrt(
        sum((float(predicted[index]) - float(actual[index])) ** 2 for index in range(count)) / float(count)
    )
# Function purpose: Normalize an iterable into a plain list of floats.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as values, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns list[float] when the function completes successfully.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

def as_float_list(values: Iterable[float]) -> list[float]:
    """Normalize an iterable into a plain list of floats."""
    return [float(value) for value in values]
