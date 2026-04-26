# Synthetic Pod Subsystem

This folder contains the software-defined pod cluster used to simulate
additional warehouse zones beyond the single physical hardware pod.

## Why This Subsystem Exists

The repository only has one real hardware pod. To demonstrate multi-pod
gateway behaviour, dashboard scaling, and connection-loss handling, the project
includes a synthetic pod subsystem that produces realistic-enough telemetry
using software.

The synthetic pods are important because they allow the system to demonstrate:

- concurrent handling of ten pods in total,
- per-pod storage and dashboard display,
- connection interruptions and reconnects,
- and forecasting across many pods without requiring many physical devices.

## Structure

- `pod2_sim.py`
  - top-level synthetic pod runner and multi-pod cluster launcher.
- `sim/`
  - environment, schedule, and fault-injection logic.
- `tests/`
  - synthetic pod tests.
- `requirements.txt`
  - runtime dependencies for the simulator.

## Design Notes

- Synthetic pods use the same logical telemetry schema as the physical pod.
- They communicate over TCP rather than BLE to keep large-scale simulation
  simpler while preserving the downstream data model.
- The subsystem is intentionally configurable enough to produce slightly
  different pod behaviour by zone profile and fault pattern.

## Limitations

- Synthetic telemetry is not equivalent to real warehouse truth.
- It is highly valuable for integration testing and demonstration scale, but it
  cannot replace long-term real environmental collection.
