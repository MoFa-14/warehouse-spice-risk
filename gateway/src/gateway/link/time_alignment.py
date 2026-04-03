"""Session-based uptime alignment and lightweight drift checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from gateway.utils.timeutils import parse_utc_iso


DEFAULT_DRIFT_THRESHOLD_S = 120.0


@dataclass
class AlignmentState:
    """Runtime alignment state for one pod session."""

    session_offset_s: float | None = None
    last_drift_s: float | None = None
    anomaly_count: int = 0


@dataclass(frozen=True)
class AlignmentResult:
    """Derived timing alignment for one sample."""

    estimated_ts_utc: datetime
    drift_s: float
    anomalous: bool


def reset_alignment(state: AlignmentState) -> None:
    """Reset alignment when a pod session restarts."""
    state.session_offset_s = None
    state.last_drift_s = None


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
