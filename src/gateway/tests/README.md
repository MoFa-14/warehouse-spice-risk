# Gateway Tests

This folder contains automated tests for the gateway package.

## Why These Tests Matter

The gateway is the system's integration point. Many failures that would appear
later as dashboard or forecasting issues actually begin here. The tests in this
folder therefore focus on protecting:

- protocol correctness,
- storage correctness,
- resampling correctness,
- forecast-runner orchestration,
- and multi-source ingestion behaviour.

## Major Test Areas

### Protocol and Validation

- `test_decoder.py`
- `test_validation.py`
- `test_reassembler.py`

These tests ensure the gateway can safely interpret pod and synthetic payloads.

### Storage and Persistence

- `test_sqlite_storage.py`
- `test_raw_writer_dedupe.py`
- `test_writer_pipeline.py`
- `test_csv_import.py`

These tests protect the database and CSV persistence paths.

### Communication and Routing

- `test_tcp_ingester.py`
- `test_multi_router.py`
- `test_gateway_interval.py`
- `test_watchdog.py`

These tests cover live ingestion and runtime coordination.

### Preprocessing

- `test_preprocess_outputs.py`
- `test_resample_grid.py`
- `test_dewpoint.py`

These tests ensure day-level processed outputs are generated consistently.

### Forecast Integration

- `test_forecast_runner.py`
- `test_forecast_cli.py`
- `test_forecast_adjustments.py`
- `test_process_lock.py`

These tests are especially important after the recent forecasting corrections
because they check the orchestration layer that connects storage, forecasting,
evaluation, and persistence.

## What These Tests Do Not Replace

They do not replace end-to-end field validation using extended real telemetry.
They prove code-path correctness, not complete forecast skill.
