# File overview:
# - Responsibility: Shared internal telemetry record used across BLE and TCP
#   ingestion.
# - Project role: Normalizes and routes telemetry arriving from multiple pods.
# - Main data or concerns: Pod identifiers, normalized records, and routing
#   decisions.
# - Related flow: Receives transport-specific records and passes per-pod outputs to
#   storage and diagnostics.
# - Why this matters: The integrated system depends on this layer to keep multi-pod
#   handling explicit rather than implicit.

"""Shared internal telemetry record used across BLE and TCP ingestion."""

from __future__ import annotations

from dataclasses import dataclass
# Class purpose: Normalized telemetry record produced by any gateway ingestion
#   source.
# - Project role: Belongs to the multi-pod routing layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The integrated system depends on this layer to keep
#   multi-pod handling explicit rather than implicit.
# - Related flow: Receives transport-specific records and passes per-pod outputs to
#   storage and diagnostics.

@dataclass(frozen=True)
class TelemetryRecord:
    """Normalized telemetry record produced by any gateway ingestion source."""

    pod_id: str
    seq: int
    ts_uptime_s: float
    temp_c: float | None
    rh_pct: float | None
    flags: int
    rssi: int | None
    source: str
    ts_pc_utc: str
