# Gateway Storage Layer

This folder contains the persistence logic for telemetry, diagnostics, and
supporting import/export workflows.

## Why This Subsystem Exists

The gateway must retain accepted telemetry and link-quality evidence so that:

- the dashboard can show live and historical conditions,
- the forecasting runner can build history windows from stored data,
- completed forecasts can later be evaluated,
- and diagnostics can explain missing or unreliable communication.

This folder implements that persistence boundary.

## Files

### `sqlite_db.py`

Core database setup file. It contains functions such as:

- `resolve_db_path`
- `connect_sqlite`
- `init_db`
- `initialize_schema`

This file matters because it defines the schema and connection behaviour used
by the live gateway database.

### `sqlite_writer.py`

Contains:

- `SqliteStorageWriter`
  - single-connection writer for telemetry and link snapshots.
- `SqliteWriterPipeline`
  - queue-backed asynchronous writer pipeline.
- request/result dataclasses that make writes explicit and testable.

This is one of the most important storage files in the project because it is
the live landing point for data accepted by the gateway.

### `sqlite_reader.py`

Read-only query helpers for retrieving latest samples, sample ranges, and
link-quality ranges directly from SQLite.

### `schema.py`

Defines quality-flag mask handling and related schema-level conventions.

### `paths.py`

Centralises storage path construction and repository-root discovery.

### `raw_writer.py`

Legacy-oriented CSV append writer path for raw telemetry persistence.

### `per_pod_csv_writer.py`

Per-pod daily CSV writing support.

### `link_writer.py`

Link-quality CSV writing support.

### `sample_csv.py`

Helpers for writing sample rows into CSV form and ensuring schema consistency.

### `import_csv.py`

Imports older CSV history into SQLite. This is important for bringing legacy
data into the newer unified database workflow.

### `export_csv.py`

Exports telemetry from SQLite back to CSV when needed for offline analysis or
handover.

## Design Choices

- SQLite is the primary live source of truth.
- The asynchronous write pipeline exists so communication tasks stay responsive
  even when persistence briefly slows or encounters recoverable failures.
- Session tracking is explicit so sequence resets after pod reboot do not
  corrupt uniqueness logic.

## Recent Behaviour Changes

- Forecasting and dashboard improvements increased the importance of SQLite as
  the integrated source of telemetry, forecast rows, and evaluation rows.
- Pod discovery now depends on more than raw telemetry tables alone, so stored
  historical pods remain visible in the dashboard.

## Limitations

- CSV support is still present for compatibility, which makes the subsystem
  broader than a pure SQLite-only design.
- The storage layer preserves evidence; it does not decide whether a row is a
  good forecasting case. That judgement is deferred to forecasting and
  evaluation logic.
