# File overview:
# - Responsibility: Per-pod link quality counters and rolling snapshots.
# - Project role: Computes communication quality, sequence gaps, and timing
#   diagnostics.
# - Main data or concerns: Sequence counters, timestamps, connectivity statistics,
#   and missing-rate metrics.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.
# - Why this matters: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.

"""Per-pod link quality counters and rolling snapshots."""

from __future__ import annotations

from dataclasses import dataclass

from gateway.utils.sequence import sequence_reset_detected
from gateway.utils.timeutils import utc_now_iso
# Class purpose: CSV-friendly link quality view for one pod.
# - Project role: Belongs to the gateway link-diagnostics layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

@dataclass(frozen=True)
class LinkSnapshot:
    """CSV-friendly link quality view for one pod."""

    ts_pc_utc: str
    pod_id: str
    connected: bool
    last_rssi: int | None
    total_received: int
    total_missing: int
    total_duplicates: int
    disconnect_count: int
    reconnect_count: int
    missing_rate: float
# Class purpose: Track connection health, packet gaps, and dedupe counters for a
#   pod.
# - Project role: Belongs to the gateway link-diagnostics layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Link-quality interpretation matters because missing data
#   changes how later telemetry should be trusted.
# - Related flow: Consumes received telemetry and passes quality summaries to
#   storage and dashboard views.

class LinkStats:
    """Track connection health, packet gaps, and dedupe counters for a pod."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as pod_label, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def __init__(self, *, pod_label: str) -> None:
        self.pod_label = pod_label
        self.pod_id = pod_label
        self.last_seen_time_utc: str | None = None
        self.last_rssi: int | None = None
        self.total_received = 0
        self.total_missing = 0
        self.total_duplicates = 0
        self.connect_count = 0
        self.disconnect_count = 0
        self.reconnect_count = 0
        self.connected = False
        self._seen_successful_connect = False
        self._last_seq: int | None = None
        self._last_uptime_s: float | None = None
    # Method purpose: Implements the update identity step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def update_identity(self, pod_id: str) -> None:
        self.pod_id = pod_id
    # Method purpose: Implements the update rssi step used by this subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as rssi, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def update_rssi(self, rssi: int | None) -> None:
        if rssi is not None:
            self.last_rssi = int(rssi)
    # Method purpose: Implements the mark connected step used by this subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def mark_connected(self) -> None:
        self.connect_count += 1
        if self._seen_successful_connect:
            self.reconnect_count += 1
        self._seen_successful_connect = True
        self.connected = True
    # Method purpose: Implements the mark disconnected step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def mark_disconnected(self) -> None:
        if self.connected:
            self.disconnect_count += 1
        self.connected = False
    # Method purpose: Implements the should reset sequence step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as seq, ts_uptime_s, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def should_reset_sequence(self, *, seq: int, ts_uptime_s: float) -> bool:
        return sequence_reset_detected(
            last_seq=self._last_seq,
            last_uptime_s=self._last_uptime_s,
            seq=int(seq),
            ts_uptime_s=float(ts_uptime_s),
        )
    # Method purpose: Implements the reset sequence tracking step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def reset_sequence_tracking(self) -> None:
        self._last_seq = None
        self._last_uptime_s = None
    # Method purpose: Implements the note duplicate step used by this subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def note_duplicate(self) -> None:
        self.total_duplicates += 1
    # Method purpose: Implements the note received step used by this subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as seq, ts_uptime_s, seen_time_utc, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: Returns int when the function completes successfully.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def note_received(self, *, seq: int, ts_uptime_s: float, seen_time_utc: str | None = None) -> int:
        missing = 0
        if self._last_seq is not None and seq > self._last_seq + 1:
            missing = seq - self._last_seq - 1
            self.total_missing += missing

        self.total_received += 1
        if self._last_seq is None or seq >= self._last_seq:
            self._last_seq = seq
        if self._last_uptime_s is None or ts_uptime_s >= self._last_uptime_s:
            self._last_uptime_s = ts_uptime_s
        self.last_seen_time_utc = seen_time_utc or utc_now_iso()
        return missing
    # Method purpose: Implements the snapshot step used by this subsystem.
    # - Project role: Belongs to the gateway link-diagnostics layer and acts as
    #   a method on LinkStats.
    # - Inputs: Arguments such as ts_pc_utc, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns LinkSnapshot when the function completes successfully.
    # - Important decisions: Link-quality interpretation matters because missing
    #   data changes how later telemetry should be trusted.
    # - Related flow: Consumes received telemetry and passes quality summaries
    #   to storage and dashboard views.

    def snapshot(self, *, ts_pc_utc: str | None = None) -> LinkSnapshot:
        timestamp = ts_pc_utc or utc_now_iso()
        denominator = self.total_received + self.total_missing
        missing_rate = (self.total_missing / denominator) if denominator else 0.0
        return LinkSnapshot(
            ts_pc_utc=timestamp,
            pod_id=self.pod_id,
            connected=self.connected,
            last_rssi=self.last_rssi,
            total_received=self.total_received,
            total_missing=self.total_missing,
            total_duplicates=self.total_duplicates,
            disconnect_count=self.disconnect_count,
            reconnect_count=self.reconnect_count,
            missing_rate=missing_rate,
        )
