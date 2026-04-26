# Synthetic Pod Tests

This folder contains automated tests for the synthetic pod subsystem.

## Purpose

The synthetic pod cluster is part of the system demonstration. If the simulator
produces invalid ranges, unrealistic schedules, or broken cluster startup
behaviour, the gateway and dashboard demo can become misleading or unstable.

## Test Files

- `test_generator_ranges.py`
  - validates output ranges.
- `test_schedule_rate.py`
  - checks schedule timing behaviour.
- `test_weather_trend.py`
  - checks weather-driven target behaviour.
- `test_event_spike_recovery.py`
  - validates disturbance and recovery handling.
- `test_burst_loss.py`
  - checks communication-loss simulation behaviour.
- `test_timezone_fallback.py`
  - validates timezone handling.
- `test_multi_pod_cluster.py`
  - covers the multi-pod expansion used to simulate pods `02` to `10`.

These tests are especially valuable because the synthetic pods influence both
the apparent load on the gateway and the interpretation of multi-pod dashboard
behaviour.
