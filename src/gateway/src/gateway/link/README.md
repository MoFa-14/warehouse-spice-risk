# Link Diagnostics and Timing

This folder contains communication-health and timing-alignment logic used by the
gateway.

## Responsibility

The repository does not treat telemetry as trustworthy simply because a packet
arrived. Communication quality is part of system interpretation. This subsystem
tracks:

- missing counts,
- duplicate counts,
- disconnect and reconnect history,
- received signal strength indication trends,
- and timing-alignment information between pod-side and gateway-side clocks.

## Files

### `stats.py`

Contains:

- `LinkSnapshot`
- `LinkStats`

These structures hold the gateway's current view of link quality for a pod.

### `time_alignment.py`

Contains:

- `AlignmentState`
- `AlignmentResult`
- `align_sample`
- `reset_alignment`

This file exists because pod uptime timestamps and gateway timestamps represent
different clocks. Alignment logic helps the gateway reason about sequence timing
and missing intervals.

### `diagnostics.py`

Contains:

- `PodDiagnosticsSummary`
- `build_diagnostics_summary`
- `diagnostics_in_range`

This file turns stored telemetry and link history into diagnostic summaries used
for review and health analysis.

## Why It Matters

This subsystem is important because a monitoring system must distinguish between
environmental behaviour and communication behaviour. Without link diagnostics, a
forecasting or dashboard anomaly could be misread as a warehouse condition when
it is actually a transport problem.
