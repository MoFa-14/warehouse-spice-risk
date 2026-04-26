# Gateway Forecast Orchestration

This folder contains the gateway-side forecasting runtime. The forecasting
package under `ml/src/forecasting/` defines the model logic and data structures,
but this folder is where those capabilities are applied to live stored data.

## Responsibility

This subsystem is responsible for:

- loading the most recent telemetry history from storage,
- constructing forecast-ready windows,
- calling the forecasting package,
- persisting forecast scenarios,
- evaluating completed forecast windows against realised data,
- maintaining the analogue case base,
- and applying calibration-eligibility and persistence-comparison logic.

## Files

### `storage_adapter.py`

Bridges raw stored telemetry and the forecasting package. It prepares the
3-hour, 1-minute-grid history windows used by the forecaster and by later
historical analysis tooling.

### `runner.py`

The central forecasting orchestration file. It controls:

- when a forecast cycle runs,
- how history is loaded,
- when event detection and baseline generation occur,
- when evaluations are due,
- when case-base updates are written,
- and how calibration information is applied or filtered.

This is one of the most important files in the repository for understanding how
the forecasting system actually operates over time.

### `outputs.py`

Persists forecasting artefacts to storage. It writes:

- forecast scenario rows,
- evaluation rows,
- case-base rows,
- and supporting fields such as persistence-comparison metrics.

### `__init__.py`

Package marker.

## Recent Behaviour Changes

This folder contains many of the recent forecasting corrections:

- separate forecast-runner locking so the forecast loop can coexist with the
  live gateway process,
- persistence-based evaluation storage,
- filtering of unsuitable evaluation rows from calibration influence,
- and support for the historical forecasting analysis shown in the dashboard.

## Dependencies

Depends on:

- `ml/src/forecasting/`
- `gateway/src/gateway/storage/`
- `gateway/src/gateway/link/`

Consumed by:

- `gateway/src/gateway/cli/forecast_cli.py`
- dashboard prediction and forecast-test services through stored outputs.

## Limitations

- Forecast quality still depends heavily on the historical case base.
- Stored historical forecast rows may remain visible even after the live model
  logic is improved, because the repository preserves what was generated at the
  time.
