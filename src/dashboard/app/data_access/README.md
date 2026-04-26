# Dashboard Data Access Layer

This folder contains the read-only data-access helpers used by the dashboard.

## Why This Subsystem Exists

The dashboard should not embed raw SQL queries or file-discovery logic directly
inside route or rendering code. This layer isolates data loading so the service
modules can focus on interpretation and presentation.

## Files

### `sqlite_reader.py`

Reads live telemetry and link-quality history from SQLite and normalises it into
pandas data frames. It also computes dew point from stored temperature and
relative humidity when needed for dashboard display.

### `forecast_reader.py`

Reads stored forecast rows and evaluation history. This file is critical for the
prediction page and the historical forecasting test tools because those pages
must interpret what the gateway stored rather than recompute the model.

### `file_finder.py`

Discovers pod folders and date-based CSV files. This module matters for
compatibility with CSV fallback paths and for pod discovery that must remain
aware of stored-but-disconnected pods.

### `csv_reader.py`

Loads raw, processed, and link-quality CSV files into pandas data frames. This
supports legacy and offline analysis workflows.

## Design Choices

- SQLite is preferred when available, but CSV support remains present for
  compatibility and historical analysis.
- Normalisation happens here so service-layer code can assume consistent column
  types.
