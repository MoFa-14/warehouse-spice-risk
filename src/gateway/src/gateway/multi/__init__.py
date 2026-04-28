# File overview:
# - Responsibility: Multi-pod gateway orchestration helpers.
# - Project role: Normalizes and routes telemetry arriving from multiple pods.
# - Main data or concerns: Pod identifiers, normalized records, and routing
#   decisions.
# - Related flow: Receives transport-specific records and passes per-pod outputs to
#   storage and diagnostics.
# - Why this matters: The integrated system depends on this layer to keep multi-pod
#   handling explicit rather than implicit.

"""Multi-pod gateway orchestration helpers."""

__all__ = []
