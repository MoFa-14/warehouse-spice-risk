# Forecasting Package Root

This folder contains the repository's forecasting-specific Python package and
its direct tests.

## Purpose

The forecasting package isolates the model logic from the gateway runtime. This
separation is useful because:

- the gateway should orchestrate forecasting, not define all model mathematics,
- the forecasting logic becomes easier to test independently,
- and the package can express forecasting concepts in its own domain language
  such as feature vectors, trajectories, cases, and evaluation metrics.

## Structure

- `src/forecasting/`
  - forecasting models, feature extraction, event detection, analogue matching,
    scenarios, and evaluation logic.
- `tests/`
  - subsystem tests focused on forecasting behaviour.
- `requirements.txt`
  - intentionally minimal dependency footprint.

## Relationship to the Rest of the Project

The forecasting package does not read raw pod transport payloads directly and it
does not render the dashboard. It sits between those worlds:

- the gateway prepares forecast-ready windows,
- this package interprets them and produces trajectories,
- the gateway persists the outputs,
- and the dashboard later reads and visualises them.

For the detailed internals, see:

- `ml/src/forecasting/README.md`
- `ml/tests/README.md`
