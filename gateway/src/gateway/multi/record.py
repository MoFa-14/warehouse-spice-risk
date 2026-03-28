"""Shared internal telemetry record used across BLE and TCP ingestion."""

from __future__ import annotations

from dataclasses import dataclass


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
