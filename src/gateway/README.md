# Gateway Package

This folder contains the gateway package and the runtime artefacts it produces
while collecting telemetry. The gateway is the operational centre of the
project. It receives physical and synthetic telemetry, validates it, persists
it, computes diagnostics, and coordinates the forecasting loop.

## Why the Gateway Exists

The pods do not store long-term history or perform analytics themselves. The
gateway exists to centralise responsibilities that are easier to manage on a
host computer:

- device discovery and connection management,
- decoding and validation,
- duplicate handling and session tracking,
- storage into a relational database,
- link-quality diagnostics,
- preprocessing and export,
- short-horizon forecasting,
- and forecast evaluation after the future arrives.

Without the gateway, the repository would contain isolated pod software and a
dashboard with no reliable unified data source.

## Folder Structure

- `src/gateway/`
  - the main Python package containing ingestion, routing, storage, and
    forecasting integration.
- `tests/`
  - automated tests for gateway behaviour.
- `raw/`, `processed/`, `db/`, `exports/`, `logs/`
  - runtime data and export locations used by the gateway.
- `pyproject.toml`
  - packaging metadata for the gateway package.
- `tools/`
  - helper scripts and support utilities used during development.

## Important Package Areas

The code under `src/gateway/` is organised by responsibility:

- `ble/`
  - BLE scanning, GATT profile handling, and pod session logic.
- `ingesters/`
  - source-specific ingestion wrappers for BLE and synthetic TCP telemetry.
- `protocol/`
  - payload decoding, newline-delimited JSON handling, and validation.
- `multi/`
  - shared queue record types, routing, and orchestration for multiple pods.
- `storage/`
  - SQLite schema, writers, readers, and CSV import/export helpers.
- `forecast/`
  - gateway-side forecasting orchestration and persistence.
- `preprocess/`
  - cleaning and resampling utilities used for processed outputs and datasets.
- `link/`
  - link-quality statistics and diagnostics.
- `cli/`
  - command-line entry points for gateway and forecasting operations.

Each of those folders now contains a dedicated local `README.md`.

## Design Choices

- The gateway treats BLE and synthetic TCP as two ingestion front ends that
  converge into one internal telemetry record format.
- SQLite is used as the live system database to keep the architecture
  inspectable and portable.
- Forecast generation happens from stored telemetry rather than directly from
  transient in-memory packets. This makes historical review and evaluation
  possible.

## Recent Behaviour Changes

Recent forecasting-related gateway work includes:

- separating the forecast loop lock from the live gateway lock,
- storing persistence-comparison metrics for completed evaluations,
- filtering unsuitable completed windows from calibration influence,
- and improving the forecasting orchestration so one automatic runner can keep
  retraining, evaluating, and storing outputs continuously.

## Cautions

- Some older stored forecast rows may still reflect earlier model behaviour.
- The gateway includes both live runtime paths and offline utility paths. When
  reading the code, it is important to distinguish the live ingestion/storage
  pipeline from reporting or export helpers.

For the package internals, see:

- `gateway/src/gateway/README.md`
- `gateway/tests/README.md`
