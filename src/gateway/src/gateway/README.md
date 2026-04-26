# Gateway Python Package

This folder contains the Python package that implements the live gateway. It is
the architectural bridge between pod telemetry and every downstream artefact:
stored history, forecasts, evaluations, exports, and dashboard-readable data.

## Responsibilities

The package is responsible for:

- discovering and communicating with pods,
- decoding and validating telemetry,
- tracking link-quality health,
- routing multi-pod records through one pipeline,
- persisting telemetry and diagnostics,
- building forecast-ready history windows,
- coordinating forecast generation and later evaluation,
- and exposing command-line entry points for live operation.

## Key Top-Level Files

### `main.py`

High-level gateway runtime composition. This file coordinates the larger pieces
of the gateway package and is useful when a reader wants to understand the live
application entry point beyond the command-line wrappers.

### `config.py`

Defines gateway settings and configuration defaults. These settings control
communication intervals, storage paths, and validation limits.

### `firmware_config_loader.py`

Loads firmware-related configuration information needed by the gateway to remain
compatible with the pod protocol and bit-flag definitions.

## Subpackages

### `ble/`

Handles Bluetooth Low Energy discovery, GATT profile verification, and
per-device sessions.

### `ingesters/`

Turns BLE notifications and synthetic TCP lines into the gateway's internal
telemetry record type.

### `protocol/`

Defines the boundary between on-wire payloads and typed Python records. It also
provides validation helpers so suspicious values are flagged rather than lost.

### `multi/`

Contains the record model, router, and orchestrator used when several pods are
active at once. This subsystem ensures the rest of the gateway sees one unified
multi-pod stream.

### `storage/`

Implements SQLite persistence, queue-backed writes, schema management, and
legacy import/export support.

### `forecast/`

Owns gateway-side forecasting orchestration: history loading, forecast loop
execution, evaluation, calibration support, and persistence of forecast
products.

### `preprocess/`

Contains cleaning, dew-point computation, resampling, and processed-file export
utilities used for datasets and historical analysis.

### `link/`

Tracks communication quality. This includes missing-rate summaries,
disconnect/reconnect counts, alignment helpers, and diagnostics.

### `cli/`

Provides operational entry points used by scripts and direct command-line use.

## Why This Package Matters

If this package were removed, the project would still contain firmware,
forecasting logic, and a dashboard, but there would be no reliable runtime path
connecting them. The gateway is the subsystem that turns many separate project
pieces into one coherent system.
