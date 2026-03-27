# Gateway Layers 2-3 (Communication + Storage)

This package now covers the live Layer 2 BLE receiver plus Layer 3 storage and preprocessing. It does not implement Layer 4 model inference, dashboards, alerts, Flask routes, or risk rules.

## Python Recommendation

Use Python 3.11 or 3.12 on Windows. These versions are the most reliable target for `bleak` on Windows BLE stacks, have solid `asyncio` support, and avoid the older compatibility issues that show up more often on Python 3.9 and 3.10.

## Setup

From the repository root:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .\gateway
```

If the `py` launcher is not available on your machine, replace `py -3.12` with your installed `python` executable.

## What Layer 3 Does

Layer 3 is responsible for:

- Persisting immutable raw telemetry as append-only CSV partitions under `data/raw/...`
- Persisting append-only gateway link-quality snapshots under `data/raw/link_quality/...`
- Running an explicit offline preprocessing pipeline that cleans, validates, resamples, and optionally interpolates data
- Exporting training-ready datasets for later ML work without doing any training in this package

The BLE gateway remains the only collector. Layer 3 never talks to BLE directly; it only stores and preprocesses the structured records produced by Layer 2.

## Source Of Truth

- Firmware source of truth: `firmware/circuitpython-pod/config.py`
- Protocol reference: `firmware/circuitpython-pod/protocol.md`
- BLE UUIDs and pod naming are loaded from the firmware config at runtime. The gateway does not duplicate those UUIDs across multiple code files.

For development and testing, the firmware sample interval is currently set to `5` seconds and the gateway requests that interval on connect so the raw store captures higher-frequency readings.

## Run Commands

Scan only:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --scan-only
```

Connect to one explicit address:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --address F2:9A:41:2B:5B:55
```

Run the receiver for 120 seconds and write compatibility logs to the default directory while also writing canonical Layer 3 storage:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --duration 120
```

Run with a custom legacy log directory:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --log-dir gateway/logs
```

Send an additional control command while connected:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --address F2:9A:41:2B:5B:55 --send "REQ_FROM_SEQ:123"
```

Optional helper tools without installing console scripts:

```powershell
.\.venv\Scripts\python.exe .\gateway\tools\scan_only.py
.\.venv\Scripts\python.exe .\gateway\tools\dump_services.py --address F2:9A:41:2B:5B:55
```

## Canonical Storage Layout

Raw telemetry is the canonical Layer 3 source of truth:

- `data/raw/pods/<pod_id>/YYYY-MM-DD.csv`
- `data/raw/link_quality/YYYY-MM-DD.csv`

Processed daily outputs are written here:

- `data/processed/pods/<pod_id>/YYYY-MM-DD_processed.csv`

Training-ready exports are written here:

- `data/exports/training_dataset.csv`

Legacy compatibility outputs are still written here for existing workflows:

- `gateway/logs/samples.csv`
- `gateway/logs/link_quality.csv`

## Raw Storage Schema

Raw telemetry columns:

- `ts_pc_utc`
- `pod_id`
- `seq`
- `ts_uptime_s`
- `temp_c`
- `rh_pct`
- `dew_point_c`
- `flags`
- `rssi`
- `quality_flags`

Link-quality columns:

- `ts_pc_utc`
- `pod_id`
- `connected`
- `last_rssi`
- `total_received`
- `total_missing`
- `total_duplicates`
- `disconnect_count`
- `reconnect_count`
- `missing_rate`

The canonical raw writers are append-only, line-buffered, and dedupe repeated `(pod_id, seq)` rows before writing.

## Writer Queue Architecture

The live gateway no longer writes CSV rows directly from the BLE notification callback.

- BLE notifications decode into structured telemetry records first.
- The BLE session then pushes those records into an in-process `asyncio.Queue`.
- A dedicated writer task consumes that queue and persists:
  - canonical raw pod partitions under `data/raw/...`
  - canonical link snapshots under `data/raw/link_quality/...`
  - compatibility logs under `gateway/logs/samples.csv` and `gateway/logs/link_quality.csv`

This separation matters for reliability on Windows: if a file write fails temporarily because of antivirus, indexing, or a transient file lock, the writer keeps retrying without killing the BLE notification path.

The writer runs with these safety behaviors:

- append mode only
- headers created automatically
- flush after every row
- full exception logging with stack traces
- automatic close, brief sleep, reopen, and retry on write errors
- heartbeat log every 10 seconds with queue and write counters

Example heartbeat:

```text
writer alive rows_written=128 write_errors=0 queue_size=0 last_write=2026-03-27T14:05:10Z
```

Interpretation:

- `rows_written`: canonical rows successfully appended by the writer
- `write_errors`: total write exceptions seen so far
- `queue_size`: backlog waiting to be flushed
- `last_write`: UTC timestamp of the last successful canonical append

If `telemetry ...` logs continue but `last_write` stops moving and `queue_size` grows, BLE is still receiving data and the writer is retrying a file problem.

## Why Raw Data Stays Immutable

We keep raw files immutable for auditability and reproducibility.

- The exact bytes received by the gateway are preserved as the permanent record for later review.
- Preprocessing can be rerun with different parameters without losing the original evidence trail.
- ML-ready datasets can always be regenerated from the same raw inputs.

## Offline Preprocessing

Preprocess one pod for one day:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli preprocess --pod 01 --date 2026-03-25
```

Preprocess every pod in a date range:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli preprocess --all --from 2026-03-01 --to 2026-03-31
```

Useful options:

- `--interval 60`
- `--interpolate`
- `--max-gap-minutes 5`

Processed output schema:

- `ts_pc_utc`
- `pod_id`
- `temp_c_clean`
- `rh_pct_clean`
- `dew_point_c`
- `missing`
- `interpolated`
- `source_seq`

Interpolation is off by default. That is intentional: we do not want to invent data unless the user explicitly opts into small-gap filling for a later analysis or ML preparation step.

## Export Training Dataset

Export a consolidated CSV from existing processed daily files:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli export-training --from 2026-03-01 --to 2026-03-31 --out data/exports/training_dataset.csv
```

The export currently keeps the feature set minimal:

- `ts_pc_utc`
- `pod_id`
- `temp_c_clean`
- `rh_pct_clean`
- `dew_point_c`
- `missing`

More advanced feature engineering should stay in the later ML layer.

## Link Metrics

- `total_received`: unique decoded samples accepted after dedupe
- `total_missing`: inferred packet loss from forward sequence gaps
- `total_duplicates`: repeated samples that were ignored by the gateway or canonical storage dedupe
- `disconnect_count`: number of observed disconnects after a successful connection
- `reconnect_count`: number of successful follow-up connections after the first one
- `missing_rate`: `total_missing / (total_received + total_missing)`
- `last_rssi`: most recent RSSI the adapter exposed through scan data

## Reliability Features

- The gateway holds a lock file at `gateway/logs/.lock` so only one process writes the same log directory at a time.
- If notifications stall while the BLE connection still looks alive, the telemetry watchdog logs a warning, forces a notify resubscribe, and then reconnects the client if telemetry still does not resume.
- Writer failures are never silent. The writer prints full exceptions and keeps retrying.
- If repeated writer failures happen in a short window, the gateway prints a `WRITER RED FLAG` message and continues retrying.

## Windows BLE Cache Notes

Windows can cache old GATT layouts for the same BLE address. This gateway defaults to uncached service discovery so it re-reads the pod's GATT layout on connect.

If Windows still appears to use stale services:

1. Close other BLE tools that might be holding the device.
2. Open `Settings > Bluetooth & devices > Devices`.
3. Find the pod and choose `Remove device`.
4. Power-cycle or reset the pod.
5. Run the gateway scan again and reconnect.

If you intentionally want to trust the cached GATT layout, add `--use-cached-services`.

## Notes

- The gateway reconnects with exponential backoff: 1s, 2s, 4s, up to 30s.
- If the pod reboots and its sequence counter restarts, the gateway clears its dedupe window and continues logging.
- The canonical Layer 3 store is CSV-first by design. A future SQLite option can be revisited later, but it is intentionally not part of this phase.

## Troubleshooting Checklist

If CSV rows stop appearing or the gateway exits with a log-directory warning:

1. Check whether another gateway process is already running and holding `gateway/logs/.lock`.
2. Watch the console for `writer alive ...` heartbeat logs.
3. If `queue_size` keeps rising, look for antivirus, OneDrive, backup software, or another tool touching the CSV files.
4. If telemetry warnings mention resubscribe or reconnect, reset the pod once and confirm the gateway reconnects.
5. Avoid opening the CSV files in a tool that may keep an exclusive lock.
6. If Windows BLE looks stale, remove the pod from Bluetooth settings and reconnect.
