# File overview:
# - Responsibility: Session-based uptime alignment and lightweight drift checks.
# - Project role: Computes communication quality, sequence gaps, and timing
#   diagnostics.
# - Main data or concerns: Sequence counters, timestamps, connectivity statistics,
#   and missing-rate metrics.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.
# - Why this matters: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.

"""Session-based uptime alignment and lightweight drift checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from gateway.utils.timeutils import parse_utc_iso


DEFAULT_DRIFT_THRESHOLD_S = 120.0
# Class purpose: Runtime alignment state for one pod session.
# - Project role: Belongs to the gateway link-diagnostics layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

@dataclass
class AlignmentState:
    """Runtime alignment state for one pod session."""

    session_offset_s: float | None = None
    last_drift_s: float | None = None
    anomaly_count: int = 0
# Class purpose: Derived timing alignment for one sample.
# - Project role: Belongs to the gateway link-diagnostics layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

@dataclass(frozen=True)
class AlignmentResult:
    """Derived timing alignment for one sample."""

    estimated_ts_utc: datetime
    drift_s: float
    anomalous: bool
# Function purpose: Reset alignment when a pod session restarts.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as state, interpreted according to the rules encoded in
#   the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def reset_alignment(state: AlignmentState) -> None:
    """Reset alignment when a pod session restarts."""
    state.session_offset_s = None
    state.last_drift_s = None
# Function purpose: Estimate uptime-to-UTC mapping for one sample and flag
#   suspicious drift.
# - Project role: Belongs to the gateway link-diagnostics layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as state, gateway_ts_utc, ts_uptime_s, drift_threshold_s,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns AlignmentResult when the function completes successfully.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

def align_sample(
    state: AlignmentState,
    *,
    gateway_ts_utc: str | datetime,
    ts_uptime_s: float,
    drift_threshold_s: float = DEFAULT_DRIFT_THRESHOLD_S,
) -> AlignmentResult:
    """Estimate uptime-to-UTC mapping for one sample and flag suspicious drift."""
    seen_time = parse_utc_iso(gateway_ts_utc) if isinstance(gateway_ts_utc, str) else gateway_ts_utc.astimezone(timezone.utc)
    if state.session_offset_s is None:
        state.session_offset_s = seen_time.timestamp() - float(ts_uptime_s)

    estimated_timestamp = datetime.fromtimestamp(float(ts_uptime_s) + float(state.session_offset_s), tz=timezone.utc)
    drift_s = (seen_time - estimated_timestamp).total_seconds()
    anomalous = abs(drift_s) > float(drift_threshold_s)
    state.last_drift_s = drift_s
    if anomalous:
        state.anomaly_count += 1
    return AlignmentResult(
        estimated_ts_utc=estimated_timestamp,
        drift_s=drift_s,
        anomalous=anomalous,
    )
