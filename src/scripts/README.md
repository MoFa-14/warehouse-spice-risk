# Operational Scripts

This folder contains convenience scripts used to run, inspect, and evaluate the
prototype.

## Why This Folder Exists

The repository contains many components that can be launched separately. These
scripts provide consistent entry points for common workflows so the project can
be demonstrated and tested without manually reconstructing long command lines.

## Files

### Runtime Launchers

- `run_gateway_multi.ps1`
  - starts the gateway in the mixed physical-plus-synthetic mode used by the
    integrated demonstration.
- `run_synthetic_pods.ps1`
  - launches the synthetic cluster for pods `02` to `10`.
- `run_pod2.ps1`
  - older compatibility launcher for the synthetic pod entry point.
- `run_dashboard.ps1`
  - starts the Flask dashboard.
- `run_forecasting_auto.py`
  - starts the unified automatic forecasting loop.

### Forecast Evaluation and Timing

- `run_forecast_evaluation.py`
  - offline reporting/evaluation helper.
- `run_latency_evaluation.py`
  - latency-related evaluation helper.

### Hardware Support

- `pod_ble_monitor.py`
  - BLE inspection and discovery helper.
- `pod_serial_monitor.py`
  - serial monitor helper for the physical pod.

## Design Notes

- These scripts are wrappers around the underlying gateway, dashboard, and
  forecasting entry points. They are operational conveniences, not alternative
  implementations of the system logic.
- The current preferred forecasting script is `run_forecasting_auto.py`, which
  reflects the unified forecast-loop design.
