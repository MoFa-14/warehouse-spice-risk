# File overview:
# - Responsibility: Sequence/session reset heuristics shared by gateway ingestion
#   and storage.
# - Project role: Provides reusable low-level helpers for timing, retry logic, and
#   sequence handling.
# - Main data or concerns: Helper arguments, timestamps, counters, and shared return
#   values.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.
# - Why this matters: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.

"""Sequence/session reset heuristics shared by gateway ingestion and storage."""

from __future__ import annotations


SOFT_RESET_UPTIME_ADVANCE_S = 15.0
# Function purpose: Return whether a pod appears to have started a new telemetry
#   session.
# - Project role: Belongs to the shared gateway utility layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as last_seq, last_uptime_s, seq, ts_uptime_s, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.

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
    if seq < last_seq and ts_uptime_s > last_uptime_s + SOFT_RESET_UPTIME_ADVANCE_S:
        return True
    return False
