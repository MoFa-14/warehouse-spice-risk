# Gateway Layers 2-3 (Communication + Storage)

This package now covers the live Layer 2 BLE receiver plus Layer 3 storage and preprocessing. SQLite in WAL mode is now the primary live storage backend so the running gateway can commit samples immediately while other processes read the database safely. It does not implement Layer 4 model inference, dashboards, alerts, Flask routes, or risk rules.

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

- Persisting live raw telemetry into `data/db/telemetry.sqlite`
- Persisting live gateway link-quality snapshots into the same SQLite database
- Running an explicit offline preprocessing pipeline that cleans, validates, resamples, and optionally interpolates data
- Exporting CSV files for dissertation/report appendices when needed
- Exporting training-ready datasets for later ML work without doing any training in this package

The BLE gateway remains the only collector. Layer 3 never talks to BLE directly; it only stores and preprocesses the structured records produced by Layer 2.

## Why SQLite WAL Mode

SQLite with `journal_mode=WAL` is the primary live store because it is much more reliable for a running gateway than append-only CSV files:

- inserts are atomic and visible immediately after commit
- readers can query while the writer keeps running
- `busy_timeout=5000` helps absorb short lock/contention bursts
- `synchronous=NORMAL` keeps durability reasonable without slowing the gateway too much

CSV is still available as a legacy backend or as an export format, but it is no longer the primary live source of truth.

## Source Of Truth

- Firmware source of truth: `firmware/circuitpython-pod/config.py`
- Protocol reference: `firmware/circuitpython-pod/protocol.md`
- BLE UUIDs and pod naming are loaded from the firmware config at runtime. The gateway does not duplicate those UUIDs across multiple code files.

For the current multi-pod development and testing workflow, the firmware sample interval is set to `10` seconds and the gateway requests that interval on connect so BLE watchdog timing and synthetic-pod timing line up.

## Run Commands

Initialize the SQLite database explicitly if you want to create it ahead of time:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli init-db --db-path data/db/telemetry.sqlite
```

Scan only:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --scan-only
```

Connect to one explicit address:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --address F2:9A:41:2B:5B:55 --storage sqlite --db-path data/db/telemetry.sqlite
```

Run the receiver for 120 seconds with SQLite as the primary store:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --duration 120 --storage sqlite --db-path data/db/telemetry.sqlite
```

Legacy CSV mode is still available if you need it:

```powershell
.\.venv\Scripts\python.exe -m gateway.main --storage csv --log-dir gateway/logs
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

## Multi-Pod Test Mode

The repo now includes a dedicated multi-pod test path that ingests:

- one real hardware pod over BLE
- one synthetic pod over TCP

Start the gateway in multi mode from the repository root:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.gateway_cli multi --ble-address F2:9A:41:2B:5B:55 --tcp-port 8765 --storage sqlite --db-path data/db/telemetry.sqlite --verbose
```

Important options:

- `--ble-address`
- `--ble-name-prefix` defaults to `SHT45-POD-`
- `--tcp-port` defaults to `8765`
- `--duration` is optional; omit it to keep the gateway running until you stop it with `Ctrl+C`
- `--log-root` defaults to `data/raw`
- `--storage` defaults to `sqlite`
- `--db-path` defaults to `data/db/telemetry.sqlite`
- `--interval-s` defaults to `10`

Gateway multi-mode output:

- prints each received record as `[pod=01 source=BLE] ...` or `[pod=02 source=TCP] ...`
- prints resend commands such as `REQ_FROM_SEQ pod=02 from_seq=21`
- prints periodic per-pod stats every 30 seconds
- mirrors accepted telemetry rows into `gateway/logs/samples.csv` for compatibility while still treating `data/raw/pods/...` as the canonical store

### 3-Terminal Multi-Pod Test

Terminal 1: hardware pod 01

- Run the Feather firmware with `SAMPLE_INTERVAL_S = 10`
- Pod name: `SHT45-POD-01`

Terminal 2: synthetic pod 02

```powershell
python .\synthetic_pod\pod2_sim.py --gateway-port 8765 --interval 10 --zone-profile entrance_disturbed --p-drop 0.1 --p-corrupt 0.05 --p-delay 0.2 --p-disconnect 0.02 --burst-loss on --verbose
```

Terminal 3: gateway

```powershell
python -m gateway.cli.gateway_cli multi --ble-address F2:9A:41:2B:5B:55 --tcp-port 8765 --verbose
```

Expected behavior during the test:

- pod `01` arrives from BLE and pod `02` arrives from TCP at the same time
- corrupt or missing TCP messages trigger resend requests back to the synthetic pod
- the synthetic pod replays buffered samples after `REQ_SEQ` or `REQ_FROM_SEQ`
- raw CSV files are written independently per pod under `data/raw/pods/...`

### Synthetic Fault Injection

The synthetic pod supports these fault flags:

- `--p-drop`
- `--p-corrupt`
- `--p-delay`
- `--p-disconnect`
- `--burst-loss`
- `--burst-duration-seconds`
- `--burst-multiplier`
- `--max-delay`
- `--disconnect-min`
- `--disconnect-max`
- `--replay-buffer-size`

These are intentionally simple probabilities so you can raise or lower stress without changing code.

The synthetic pod also supports warehouse microclimate zone profiles:

- `--zone-profile interior_stable`
- `--zone-profile entrance_disturbed`
- `--zone-profile upper_rack_stratified`

These let pod 02 emulate a visibly different warehouse micro-zone while the gateway continues ingesting pod 01 from the real hardware pod.

## Canonical Storage Layout

Primary live storage:

- `data/db/telemetry.sqlite`

Key tables:

- `samples_raw`
- `link_quality`
- `gateway_events`

The `samples_raw` table now includes an internal `session_id` so a pod restart can reuse sequence numbers without colliding with an older run in the same database. Queries and exports still present the familiar telemetry columns.

Legacy/optional CSV paths:

- `data/raw/pods/<pod_id>/YYYY-MM-DD.csv` when running with `--storage csv`
- `data/raw/link_quality/YYYY-MM-DD.csv` when running with `--storage csv`
- exported appendix/report CSV files under `data/exports/...`

Processed daily outputs are written here:

- `data/processed/pods/<pod_id>/YYYY-MM-DD_processed.csv`

Training-ready exports are written here:

- `data/exports/training_dataset.csv`

Legacy compatibility outputs are still written here only when CSV mode is selected:

- `gateway/logs/samples.csv`
- `gateway/logs/link_quality.csv`

## SQLite Commands

Print the latest row for one pod from another terminal while the gateway is running:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli latest --pod 01 --db-path data/db/telemetry.sqlite
```

Export one pod's raw rows from SQLite into a CSV appendix:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli export-csv --pod 01 --from 2026-03-01 --to 2026-03-31 --out data/exports/pod01.csv --db-path data/db/telemetry.sqlite
```

Export all pods for a date range:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli export-csv --all --from 2026-03-01 --to 2026-03-31 --out-dir data/exports --db-path data/db/telemetry.sqlite
```

Copy historical CSV telemetry into SQLite without changing the original files:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli import-csv --db-path data/db/telemetry.sqlite
```

Useful backfill options:

- `--pod 01` to import only one pod
- `--skip-link-quality` to copy only telemetry samples
- `--skip-legacy-logs` to ignore `gateway/logs/*.csv` and import only canonical `data/raw/...` files

The backfill command is idempotent. It checks the existing database first, skips rows that are already present, and leaves the source CSV files untouched.

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

The live gateway does not write directly from the BLE notification callback.

- BLE notifications decode into structured telemetry records first.
- The BLE session then pushes those records into an in-process `asyncio.Queue`.
- A dedicated SQLite writer task consumes that queue and persists rows into `samples_raw` and `link_quality`.
- In CSV mode, the legacy CSV writer path is still available, but SQLite is the default and recommended path.

This separation matters for reliability on Windows: if SQLite reports a transient failure, the writer logs the full exception, reopens the connection, and keeps retrying without killing the BLE notification path.

The SQLite writer also tracks pod restarts. If uptime rolls backward or the sequence restarts, the gateway opens a new internal SQLite session for that pod so sequence numbers can start at `1` again without being mistaken for permanent duplicates.

The writer runs with these safety behaviors:

- append mode only
- headers created automatically
- flush after every row
- full exception logging with stack traces
- automatic close, brief sleep, reopen, and retry on write errors
- heartbeat log every 10 seconds with queue and write counters

Example heartbeat:

```text
sqlite writer alive rows_written=128 commits=128 write_errors=0 queue_size=0 last_write=2026-03-27T14:05:10Z
```

Interpretation:

- `rows_written`: canonical rows successfully appended by the writer
- `commits`: successful SQLite commits performed by the writer
- `write_errors`: total write exceptions seen so far
- `queue_size`: backlog waiting to be flushed
- `last_write`: UTC timestamp of the last successful SQLite write

If `telemetry ...` logs continue but `last_write` stops moving and `queue_size` grows, BLE is still receiving data and the writer is retrying a storage problem.

## Why Raw Data Stays Immutable

We keep raw files immutable for auditability and reproducibility.

- The exact bytes received by the gateway are preserved as the permanent record for later review.
- Preprocessing can be rerun with different parameters without losing the original evidence trail.
- ML-ready datasets can always be regenerated from the same raw inputs.

## Offline Preprocessing

The current preprocessing commands still operate on raw CSV day files. If you ran the gateway in SQLite mode, export the desired date range to CSV first, or run the gateway with `--storage csv` for workflows that still depend on raw day partitions.

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

- In SQLite mode, the gateway holds a lock file next to the database, for example `data/db/telemetry.sqlite.lock`, so only one gateway process writes that database at a time.
- During the transition from the older directory-level lock, the gateway also honors a legacy `data/db/.lock` if an older process created it.
- In CSV mode, the gateway still uses `gateway/logs/.lock`.
- If notifications stall while the BLE connection still looks alive, the telemetry watchdog logs a warning, forces a notify resubscribe, and then reconnects the client if telemetry still does not resume.
- The watchdog treats only unique telemetry progress as healthy. If BLE starts replaying the same sample repeatedly, the gateway now treats that as a stall and forces a resubscribe or reconnect instead of silently freezing the live database view.
- Sequence/session reset detection also handles CircuitPython soft reloads where `seq` drops sharply but `ts_uptime_s` keeps increasing. That prevents a restarted pod from being mistaken for an old duplicate stream after the gateway reconnects mid-run.
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
- If the pod reboots and its sequence counter restarts, the gateway clears its in-memory dedupe window and opens a new internal SQLite session so logging continues instead of colliding with old rows.
- SQLite is now the primary live store. CSV is kept as a legacy backend and as an export/reporting format.

## Troubleshooting Checklist

If rows stop appearing or SQLite reports locking trouble:

1. Check whether another gateway process is already running and holding `data/db/telemetry.sqlite.lock`, the legacy `data/db/.lock`, or `gateway/logs/.lock`, depending on the selected backend.
2. Watch the console for `sqlite writer alive ...` heartbeat logs.
3. If `queue_size` keeps rising, look for antivirus, OneDrive, backup software, or another tool scanning the database heavily.
4. `busy_timeout=5000` already handles short lock bursts. If a lock error still persists, stop duplicate gateway processes first. On Windows PowerShell, `Stop-Process -Id <PID>` will stop the old gateway if its terminal is gone.
5. If telemetry warnings mention resubscribe or reconnect, reset the pod once and confirm the gateway reconnects.
6. If Windows BLE looks stale, remove the pod from Bluetooth settings and reconnect.
7. In multi-pod mode, confirm the synthetic pod is still connected to the TCP listener on `127.0.0.1:8765`.
