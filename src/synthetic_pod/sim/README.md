# Synthetic Pod Simulation Core

This folder contains the building blocks used to generate realistic-looking
synthetic telemetry and communication behaviour.

## Files

### `generator.py`

Contains:

- `MicroclimateConfig`
- `GeneratedTelemetrySample`
- `SyntheticTelemetryGenerator`

This is the main telemetry-generation engine. It combines schedule, weather,
zone profile, and noise/fault behaviour into per-sample outputs.

### `zone_profiles.py`

Contains:

- `ZoneProfile`
- `zone_profile_names`
- `get_zone_profile`

This file defines the behavioural differences between synthetic zones so the
gateway and dashboard are not simply receiving nine copies of the same series.

### `weather.py`

Contains indoor-climate target helpers such as `bristol_indoor_target`. This
file introduces seasonal or monthly context that helps the synthetic series feel
less arbitrary.

### `schedule.py`

Contains `ActiveHoursSchedule`, used to shape time-of-day or operational-hour
behaviour.

### `faults.py`

Contains:

- `FaultAction`
- `FaultProfile`
- `FaultController`

This file matters because one purpose of the synthetic cluster is to exercise
missing-data and disturbance behaviour rather than producing perfectly smooth
ideal signals.

### `buffer.py`

Contains `ReplayBuffer`, which supports retransmission-like or reconnect-aware
behaviour on the synthetic side.

## Design Notes

- The simulation is not just random number generation. It combines structural
  influences such as zone identity, time-of-day behaviour, and fault injection.
- The downstream gateway is intended to treat synthetic pods as operationally
  plausible peers, even though the communication transport differs from BLE.
