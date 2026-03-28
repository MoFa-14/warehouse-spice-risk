"""Sequence/session reset heuristics shared by gateway ingestion and storage."""

from __future__ import annotations


SOFT_RESET_SEQ_DROP_THRESHOLD = 5
SOFT_RESET_UPTIME_ADVANCE_S = 15.0


def sequence_reset_detected(
    *,
    last_seq: int | None,
    last_uptime_s: float | None,
    seq: int,
    ts_uptime_s: float,
) -> bool:
    """Return whether a pod appears to have started a new telemetry session."""
    if last_uptime_s is not None and ts_uptime_s + 1.0 < last_uptime_s:
        return True
    if last_seq is not None and seq == 1 and last_seq > 1:
        return True
    if last_seq is None or last_uptime_s is None:
        return False
    if seq + SOFT_RESET_SEQ_DROP_THRESHOLD < last_seq and ts_uptime_s > last_uptime_s + SOFT_RESET_UPTIME_ADVANCE_S:
        return True
    return False
