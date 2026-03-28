"""Per-pod link quality counters and rolling snapshots."""

from __future__ import annotations

from dataclasses import dataclass

from gateway.utils.sequence import sequence_reset_detected
from gateway.utils.timeutils import utc_now_iso


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


class LinkStats:
    """Track connection health, packet gaps, and dedupe counters for a pod."""

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

    def update_identity(self, pod_id: str) -> None:
        self.pod_id = pod_id

    def update_rssi(self, rssi: int | None) -> None:
        if rssi is not None:
            self.last_rssi = int(rssi)

    def mark_connected(self) -> None:
        self.connect_count += 1
        if self._seen_successful_connect:
            self.reconnect_count += 1
        self._seen_successful_connect = True
        self.connected = True

    def mark_disconnected(self) -> None:
        if self.connected:
            self.disconnect_count += 1
        self.connected = False

    def should_reset_sequence(self, *, seq: int, ts_uptime_s: float) -> bool:
        return sequence_reset_detected(
            last_seq=self._last_seq,
            last_uptime_s=self._last_uptime_s,
            seq=int(seq),
            ts_uptime_s=float(ts_uptime_s),
        )

    def reset_sequence_tracking(self) -> None:
        self._last_seq = None
        self._last_uptime_s = None

    def note_duplicate(self) -> None:
        self.total_duplicates += 1

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
