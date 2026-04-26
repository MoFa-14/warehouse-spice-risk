# Gateway Command-Line Interfaces

This folder contains the command-line entry points used to run the gateway,
storage utilities, and forecasting loop.

## Files

### `gateway_cli.py`

Main operational gateway entry point. It configures and launches live gateway
behaviour such as mixed physical-plus-synthetic ingestion.

Important functions:

- `parse_args`
- `configure_logging`
- `cli`

### `forecast_cli.py`

Unified forecasting entry point used by the automatic forecast runner and manual
invocation.

Important functions:

- `parse_args`
- `_add_common_arguments`
- `_validate_args`
- `configure_logging`
- `cli`

This file matters because it exposes the forecast loop cadence, single-run
execution, and storage path selection.

### `storage_cli.py`

Command-line helpers for storage-oriented tasks such as import/export or data
inspection workflows.

## Design Notes

- Command-line parsing is kept separate from the underlying services so tests
  can validate business logic independently from argument parsing.
- The unified forecasting command-line path is important because the repository
  previously contained overlapping manual forecast commands that could collide.
