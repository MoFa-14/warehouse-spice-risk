# Warehouse Spice Risk

This repository contains a warehouse environmental monitoring and short-horizon forecasting system for spice-stock preservation.

The implemented system combines:

- one real BLE hardware pod based on an Adafruit Feather nRF52840 Express and an SHT45 temperature / humidity sensor
- one synthetic TCP pod used for multi-zone testing without additional hardware
- a Python gateway that ingests both sources concurrently
- SQLite as the primary live store
- an offline preprocessing/export path
- a 3-hour input / 30-minute horizon forecasting pipeline
- a Flask dashboard for live status, alerts, health, prediction views, and review summaries

## 1. Current System Status

The current codebase is an end-to-end student project implementation with real working subsystems, not just mock interfaces.

What is implemented:

- real BLE telemetry from the physical pod
- TCP/JSON telemetry from the synthetic pod
- concurrent gateway ingestion for both sources
- validation, sequence-gap handling, duplicate handling, resend requests, and link-quality tracking
- SQLite live persistence in WAL mode
- raw/processed/export data paths
- 3-hour forecasting input windows
- 30-minute baseline and event-persist forecast outputs
- forecast evaluation with MAE and RMSE
- dashboard views for overview, pod detail, health, alerts, prediction, and monitoring review
- lightweight calibration, smoothing, monitoring review, and time-alignment diagnostics

## 2. Repository Layout

The repository root is:

- `C:\Users\TERA MAX\Desktop\DSP`

The active code workspace is:

- `C:\Users\TERA MAX\Desktop\DSP\src`

The important runtime code lives under `src`:

- `src/firmware/circuitpython-pod/`
  - embedded firmware for the physical pod
- `src/gateway/src/gateway/`
  - gateway ingestion, storage, preprocessing, forecasting integration, and CLIs
- `src/ml/src/forecasting/`
  - forecasting logic and feature extraction
- `src/dashboard/app/`
  - Flask dashboard, data access, and service layer
- `src/synthetic_pod/`
  - synthetic second pod simulator
- `src/data/`
  - live SQLite database plus raw, processed, and exported data
- `src/scripts/`
  - helper run scripts used by the project

High-level data locations:

- live SQLite database:
  - `src/data/db/telemetry.sqlite`
- optional legacy/on-demand directories:
  - `src/data/ml/`
  - `src/data/raw/`
  - `src/data/processed/`
  - `src/data/exports/`
  - these are not part of the cleaned default repo layout and are only created if JSONL fallback storage, CSV import/export, or legacy preprocessing paths are used

## 3. System Architecture

The architecture is intentionally layered.

### Layer 1: Physical pod firmware

Responsibilities:

- sample the SHT45 sensor
- maintain a monotonic sequence number within a pod session
- expose telemetry through BLE GATT notifications
- expose a control characteristic for gateway commands
- expose a status characteristic for lightweight runtime state

Important firmware files:

- `src/firmware/circuitpython-pod/code.py`
- `src/firmware/circuitpython-pod/config.py`
- `src/firmware/circuitpython-pod/ble_service.py`
- `src/firmware/circuitpython-pod/sensors.py`
- `src/firmware/circuitpython-pod/ring_buffer.py`
- `src/firmware/circuitpython-pod/status.py`

### Layer 2: Communication and ingestion

Responsibilities:

- connect to the real BLE pod
- listen for TCP telemetry from the synthetic pod
- decode and validate records
- handle sequence gaps, resend requests, reconnects, and duplicates
- keep per-pod runtime/link statistics

Important gateway ingestion files:

- `src/gateway/src/gateway/ble/`
- `src/gateway/src/gateway/ingesters/`
- `src/gateway/src/gateway/multi/router.py`
- `src/gateway/src/gateway/control/resend.py`
- `src/gateway/src/gateway/link/stats.py`

### Layer 3: Storage and preprocessing

Responsibilities:

- persist live telemetry and link snapshots into SQLite
- preserve raw telemetry for audit/replay
- support CSV compatibility and export
- preprocess and resample daily datasets
- backfill CSV history into SQLite

Important files:

- `src/gateway/src/gateway/storage/sqlite_db.py`
- `src/gateway/src/gateway/storage/sqlite_writer.py`
- `src/gateway/src/gateway/storage/sqlite_reader.py`
- `src/gateway/src/gateway/storage/schema.py`
- `src/gateway/src/gateway/preprocess/`

### Layer 4: Forecasting and presentation

Responsibilities:

- read stored telemetry windows
- detect recent disturbances
- generate baseline and event-persist forecasts
- evaluate forecasts after the horizon completes
- store forecasts, evaluations, and analogue cases
- present readings, alerts, health, prediction, and review summaries in Flask

Important files:

- `src/gateway/src/gateway/forecast/runner.py`
- `src/gateway/src/gateway/forecast/storage_adapter.py`
- `src/ml/src/forecasting/`
- `src/dashboard/app/main.py`
- `src/dashboard/app/services/`

## 4. End-to-End Runtime Flow

The implemented telemetry path is:

1. The physical pod samples temperature and RH on the Feather nRF52840.
2. The pod emits JSON telemetry over a custom BLE service.
3. The synthetic pod generates realistic warehouse telemetry and sends JSON over TCP.
4. The gateway ingests both streams in multi-pod mode.
5. The router validates samples, tracks sequence quality, handles missing/duplicate logic, and triggers resend requests where needed.
6. The gateway writes telemetry and link snapshots into SQLite.
7. The forecasting runner reads the latest 3-hour window from storage.
8. The forecasting layer writes the latest forecasts and later writes evaluations and case-base rows.
9. The Flask dashboard reads the stored telemetry and forecast outputs and renders pages.

## 5. Physical Pod Firmware

Target hardware:

- Adafruit Feather nRF52840 Express
- Sensirion SHT45 / SHT4x-family sensor

Firmware characteristics:

- the BLE device name comes from firmware config
- the pod emits JSON telemetry payloads
- telemetry contains:
  - `pod_id`
  - `seq`
  - `ts_uptime_s`
  - `temp_c`
  - `rh_pct`
  - `flags`
- the pod maintains a ring buffer so the gateway can request missing sequence ranges later

Firmware configuration is loaded by the gateway from:

- `src/firmware/circuitpython-pod/config.py`

This avoids hard-coding BLE UUIDs separately in multiple places.

## 6. Synthetic Pod

The synthetic pod acts as a second warehouse zone for multi-pod testing.

Responsibilities:

- simulate a second telemetry source without additional hardware
- emit JSON over TCP
- model warehouse-zone variability
- inject communication faults on purpose
- respond to resend requests from the gateway

Important files:

- `src/synthetic_pod/pod2_sim.py`
- `src/synthetic_pod/sim/generator.py`
- `src/synthetic_pod/sim/faults.py`
- `src/synthetic_pod/sim/weather.py`
- `src/synthetic_pod/sim/zone_profiles.py`
- `src/synthetic_pod/sim/schedule.py`

Implemented realism features include:

- day/night behavior
- seasonal tendency
- zone profiles such as interior, entrance-disturbed, and upper-rack behavior
- drop, delay, corruption, disconnect, and burst-loss fault injection

## 7. Gateway Ingestion

The gateway is the shared ingestion point for both telemetry sources.

The key runtime path is centered around:

- `src/gateway/src/gateway/multi/router.py`

The router is responsible for:

- validating each telemetry record
- tracking duplicates
- detecting sequence gaps
- issuing resend requests
- detecting session resets
- tracking per-pod link stats
- passing accepted rows to storage

The current system supports:

- BLE hardware pod ingestion
- TCP synthetic pod ingestion
- concurrent multi-pod mode

### Sequence and session handling

The code uses:

- sequence numbers from the pod payload
- `ts_uptime_s`
- `session_id` in SQLite

This allows the system to:

- distinguish true duplicates from restarted sessions
- reopen a new logical session when sequence numbers reset or when uptime/sequence behavior suggests a reboot or soft reset

Key reset logic lives in:

- `src/gateway/src/gateway/utils/sequence.py`

## 8. SQLite Storage Model

SQLite is the primary live data store.

Key reasons:

- simple deployment
- no external database server
- reliable concurrent read/write with WAL mode
- enough structure for telemetry, link-quality, forecasts, and evaluations

### Core tables

#### `samples_raw`

Purpose:

- primary audit-friendly telemetry table

Important columns:

- `ts_pc_utc`
- `pod_id`
- `session_id`
- `seq`
- `ts_uptime_s`
- `temp_c`
- `rh_pct`
- `flags`
- `rssi`
- `quality_flags`
- `source`

Primary key:

- `(pod_id, session_id, seq)`

This preserves raw values and allows repeated sequence values after a session reset.

#### `link_quality`

Purpose:

- periodic per-pod communication health snapshots

Important columns:

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

#### `gateway_events`

Purpose:

- operational gateway event log

Used for things like:

- resend-request logging
- timing anomaly warnings
- other gateway runtime warnings/errors

#### `forecasts`

Purpose:

- store per-pod forecast artifacts

Important columns:

- `ts_pc_utc`
- `pod_id`
- `scenario`
- `horizon_min`
- `json_forecast`
- `json_p25`
- `json_p75`
- `event_detected`
- `event_type`
- `event_reason`
- `model_version`

#### `evaluations`

Purpose:

- store completed post-horizon forecast quality metrics

Important columns:

- `ts_forecast_utc`
- `pod_id`
- `scenario`
- `MAE_T`
- `RMSE_T`
- `MAE_RH`
- `RMSE_RH`
- `event_detected`
- `large_error`
- `notes`

The code also supports case-base storage for analogue forecasting through the forecasting layer.

## 9. Validation and Quality Flags

The gateway preserves validation state using quality flags instead of silently discarding information.

Examples of implemented quality flags:

- `temp_missing`
- `temp_out_of_range`
- `rh_missing`
- `rh_out_of_range`
- `sensor_error`
- `low_batt`
- `sequence_reset`
- `seq_gap`
- `json_error_fixed`
- `duplicate`
- `time_sync_anomaly`

Quality-flag helpers live in:

- `src/gateway/src/gateway/storage/schema.py`

## 10. Calibration, Compensation, Filtering, and Smoothing

This area was extended to better align the implementation with the literature-review claim around calibration and signal conditioning.

### Per-pod calibration

The current code now supports optional per-pod calibration offsets loaded from a JSON file.

Default lookup path:

- `src/data/config/telemetry_adjustments.json`

Important note:

- this path is optional and may not exist in a fresh checkout
- you can create it yourself or point the runtime at another file path
- the dashboard can read the default path through `DSP_TELEMETRY_ADJUSTMENTS_PATH`
- the forecasting CLI can also be given an explicit file with `--telemetry-adjustments`

Supported calibration settings:

- `temp_offset_c`
- `rh_offset_pct`

Important behavior:

- raw values in `samples_raw` remain unchanged
- calibration is applied only in derived downstream read paths
- dew point is recomputed from calibrated temperature and RH when calibration is active

Current read paths that use this:

- dashboard latest reading service
- dashboard time-series read path
- forecast input storage adapter

### Optional smoothing

The current code supports lightweight optional smoothing using:

- rolling mean

This is configurable separately for:

- forecast input windows
- dashboard chart rendering

The design intent is:

- keep raw storage untouched
- allow cleaner display and more stable forecast inputs when needed
- avoid over-complicating the architecture

### Example adjustment config

```json
{
  "default": {
    "temp_offset_c": 0.0,
    "rh_offset_pct": 0.0
  },
  "pods": {
    "01": {
      "temp_offset_c": 0.3,
      "rh_offset_pct": -1.5
    }
  },
  "forecast_smoothing": {
    "enabled": true,
    "method": "rolling_mean",
    "window": 3
  },
  "dashboard_smoothing": {
    "enabled": true,
    "method": "rolling_mean",
    "window": 3
  }
}
```

## 11. Dew Point Handling

Dew point is a derived feature, not a primary raw measurement.

It is computed from temperature and RH in multiple places:

- preprocessing
- dashboard raw/processed read normalization
- forecast input preparation
- forecast trajectories

Important implementation rule:

- if calibrated temperature/RH values are active, dew point should be based on those corrected values, not the raw values

## 12. Preprocessing and Export

The project includes an explicit offline preprocessing path.

Main preprocessing responsibilities:

- read raw CSV telemetry
- apply range cleaning
- resample to a uniform grid
- optionally interpolate small gaps
- write processed daily CSV outputs

Important files:

- `src/gateway/src/gateway/preprocess/clean.py`
- `src/gateway/src/gateway/preprocess/resample.py`
- `src/gateway/src/gateway/preprocess/export.py`

This preprocessing path is separate from live ingestion and does not replace SQLite as the live source of truth.

## 13. Forecasting Subsystem

The forecasting subsystem uses:

- 3 hours of history
- 1-minute resampling
- 30-minute forecast horizon

### Core design

The current implementation is continuous-value forecasting, not classification.

It predicts:

- future temperature trajectory
- future RH trajectory
- future dew-point trajectory

### Event detection

The forecaster first checks whether the recent history contains an unusual event.

Implemented event logic includes:

- recent slope analysis
- robust thresholds
- spike detection
- event labels such as:
  - `door_open_like`
  - `ventilation_issue_like`
  - `unknown`

### Baseline and event-persist outputs

If no event is detected:

- only the baseline forecast is produced

If an event is detected:

- the baseline forecast is produced from the event-robust filtered history
- an `event_persist` scenario is also produced

### Case-base / analogue forecasting

The baseline forecast uses analogue matching where enough historical cases are available.

Implemented pipeline pieces include:

- feature extraction from the 3-hour window
- nearest-neighbor case lookup
- forecast aggregation
- percentile-band output
- fallback persistence-style behavior when the case base is too small

Important files:

- `src/ml/src/forecasting/features.py`
- `src/ml/src/forecasting/event_detection.py`
- `src/ml/src/forecasting/filtering.py`
- `src/ml/src/forecasting/knn_forecaster.py`
- `src/ml/src/forecasting/evaluator.py`
- `src/ml/src/forecasting/case_base.py`

### Evaluation and feedback

After the 30-minute horizon completes, the system evaluates forecast quality.

Metrics:

- MAE temperature
- RMSE temperature
- MAE RH
- RMSE RH

Evaluation results are stored and can also feed case-base growth for future analogue matching.

## 14. Dashboard

The Flask dashboard lives in:

- `src/dashboard/app/`

Main route file:

- `src/dashboard/app/main.py`

The dashboard is presentation-only. It does not compute live forecasting itself.

### Pages

#### `/`

Overview page with:

- latest reading per pod
- derived alert state
- latest dew point
- status/recommendation text

#### `/pods/<pod_id>`

Pod detail page with:

- latest pod reading
- historical charts
- preset or custom review windows
- threshold legend
- latest pod forecast panel

#### `/health`

Health page with:

- per-pod connectivity state
- RSSI
- total received
- missing count
- duplicate count
- missing rate

#### `/alerts`

Alerts page with:

- active alerts
- acknowledged alerts
- acknowledgement actions

Alert acknowledgements are lightweight UI/runtime state and live in:

- `src/dashboard/app/runtime/acks.json`

#### `/prediction`

Prediction page with:

- latest stored forecasts for all pods
- baseline and event-persist scenarios
- forecast charts
- derived predicted status
- recommendation text
- evaluation metrics where available

#### `/review`

Monitoring review page added in the recent work.

This page provides a lightweight environmental monitoring review summary over a selected window.

It summarizes:

- pod scope
- sample count
- threshold excursion count
- worst severity seen
- temperature trend
- RH trend
- missing-sample summary
- duplicate summary
- reconnect summary
- recommendation-triggering event count
- gateway warning count
- active acknowledgement count

This is intentionally review support, not compliance/certification logic.

## 15. Monitoring Review Support

The monitoring review feature was added to align the system more closely with risk-based environmental monitoring and periodic review concepts.

It is implemented as:

- dashboard service:
  - `src/dashboard/app/services/review_service.py`
- dashboard page:
  - `/review`
- small CLI:
  - `src/dashboard/app/review_cli.py`

The review summary reads from SQLite and produces an auditable, time-bounded summary instead of a vague UI-only status screen.

## 16. Time Alignment, Drift, and Diagnostics

The current system does not implement full network-wide clock synchronization.

What it does implement now:

- session-based mapping from pod uptime to gateway UTC
- reset of alignment when a new session is detected
- timing anomaly detection
- per-pod diagnostics summary
- gateway event logging for resend requests and time-sync anomalies

Important files:

- `src/gateway/src/gateway/link/time_alignment.py`
- `src/gateway/src/gateway/link/diagnostics.py`
- `src/gateway/src/gateway/multi/router.py`

### Why this matters

This is the feasible part of the literature-review theme around timing integrity:

- use pod uptime plus gateway receive time
- estimate whether timestamps stay consistent inside a session
- flag suspicious drift or inconsistency
- surface the result in a diagnostics summary

### Diagnostics summary

The diagnostics CLI summarizes, per pod:

- sample count
- session count
- average/min/max RSSI
- missing samples
- duplicates
- reconnects
- resend requests
- drift anomaly count
- maximum absolute drift
- latest estimated sample time

## 17. BLE Mesh: What The Code Actually Does

The README should be explicit here because the literature review may discuss BLE mesh and wider communication robustness.

The current codebase does **not** implement BLE mesh.

Current implemented communication model:

- one direct BLE hardware pod connection
- one TCP synthetic pod connection
- one gateway process that ingests both

What the recent changes do provide, which are relevant to future mesh-like scaling work:

- stronger multi-pod runtime routing
- per-pod diagnostics
- drift and timing integrity checks
- clearer session handling
- resend and replay logic

So the project is closer to:

- gateway-side timing and diagnostics support for multi-pod systems

It is not a BLE mesh implementation.

## 18. Commands

The commands below assume the working directory is:

- `C:\Users\TERA MAX\Desktop\DSP\src`

### Environment setup

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .\gateway
.\.venv\Scripts\python.exe -m pip install -r .\dashboard\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\synthetic_pod\requirements.txt
```

### Run the dashboard

```powershell
.\scripts\run_dashboard.ps1
```

### Run the gateway in multi-pod mode

```powershell
.\scripts\run_gateway_multi.ps1
```

Or manually:

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.gateway_cli multi --tcp-port 8765 --storage sqlite --db-path data/db/telemetry.sqlite --verbose
```

### Run the synthetic pod

```powershell
.\scripts\run_pod2.ps1
```

Or manually:

```powershell
.\.venv\Scripts\python.exe .\synthetic_pod\pod2_sim.py --gateway-host 127.0.0.1 --gateway-port 8765 --pod-id 02 --interval 60 --zone-profile entrance_disturbed --verbose
```

### Initialize the SQLite database

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli init-db --db-path data/db/telemetry.sqlite
```

### Query the latest sample for one pod

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.storage_cli latest --pod 01 --db-path data/db/telemetry.sqlite
```

### Run one forecast cycle

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.forecast_cli once --all --storage sqlite --db-path data/db/telemetry.sqlite
```

### Run recurring forecasts

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.forecast_cli run --all --every-minutes 30 --storage sqlite --db-path data/db/telemetry.sqlite
```

### Run a forecast with telemetry adjustments

```powershell
.\.venv\Scripts\python.exe -m gateway.cli.forecast_cli once --all --storage sqlite --db-path data/db/telemetry.sqlite --telemetry-adjustments data/config/telemetry_adjustments.json
```

### Run the monitoring review CLI

From `src`:

```powershell
cd .\dashboard
..\.venv\Scripts\python.exe -m app.review_cli --range 7d
```

Note:

- add `--pod 01` if you want a single-pod review window
- depending on your environment, it may be easier to run from `src\dashboard` using a system Python interpreter if the local venv path is not set up the same way on your machine

### Run the diagnostics summary

From `src/gateway`:

```powershell
$env:PYTHONPATH='C:\Users\TERA MAX\Desktop\DSP\src\gateway\src'
& 'C:\Users\TERA MAX\AppData\Local\Python\pythoncore-3.12-64\python.exe' -m gateway.cli.storage_cli diagnostics --hours 24 --db-path 'C:\Users\TERA MAX\Desktop\DSP\src\data\db\telemetry.sqlite'
```

## 19. Tests

Main automated tests live in:

- `src/dashboard/tests/`
- `src/gateway/tests/`
- `src/ml/tests/`
- `src/synthetic_pod/tests/`

Run them from the repository root:

```powershell
& 'C:\Users\TERA MAX\AppData\Local\Python\pythoncore-3.12-64\python.exe' -m unittest discover -s src\dashboard\tests
& 'C:\Users\TERA MAX\AppData\Local\Python\pythoncore-3.12-64\python.exe' -m unittest discover -s src\gateway\tests
& 'C:\Users\TERA MAX\AppData\Local\Python\pythoncore-3.12-64\python.exe' -m unittest discover -s src\ml\tests
& 'C:\Users\TERA MAX\AppData\Local\Python\pythoncore-3.12-64\python.exe' -m unittest discover -s src\synthetic_pod\tests
```

Recent verified status at the time of this README rewrite:

- dashboard tests: `21/21` passing
- gateway tests: `48/48` passing
- ML tests: `7/7` passing
- synthetic pod tests: `11/11` passing
- overall verified total: `87/87` passing

## 20. Recommended Startup Order

For a full end-to-end demo from a clean terminal setup:

1. Initialize SQLite if the database does not exist yet.
2. Start the gateway in multi-pod mode.
3. Start the synthetic pod.
4. Start the Flask dashboard.
5. Run the forecast CLI once or on a repeating 30-minute loop.

This order keeps the gateway as the ingestion anchor, the database as the live source of truth, and the dashboard as a reader of persisted state.

## 21. Main Code Map

### Dashboard

- `src/dashboard/app/main.py`
  - Flask application factory and routes
- `src/dashboard/app/data_access/`
  - SQLite and CSV readers used by the UI
- `src/dashboard/app/services/pod_service.py`
  - latest-reading logic
- `src/dashboard/app/services/link_service.py`
  - health page logic
- `src/dashboard/app/services/prediction_service.py`
  - forecast rendering logic
- `src/dashboard/app/services/review_service.py`
  - monitoring review summary generation
- `src/dashboard/app/services/telemetry_adjustments.py`
  - calibration and smoothing on dashboard read paths

### Gateway

- `src/gateway/src/gateway/cli/gateway_cli.py`
  - gateway multi-pod CLI
- `src/gateway/src/gateway/cli/storage_cli.py`
  - storage, export, import, and diagnostics CLI
- `src/gateway/src/gateway/cli/forecast_cli.py`
  - forecasting CLI
- `src/gateway/src/gateway/multi/router.py`
  - normalized record routing, resend, and timing anomaly handling
- `src/gateway/src/gateway/storage/sqlite_writer.py`
  - SQLite write path
- `src/gateway/src/gateway/link/diagnostics.py`
  - diagnostics summary logic
- `src/gateway/src/gateway/link/time_alignment.py`
  - uptime-to-UTC alignment

### Forecasting

- `src/gateway/src/gateway/forecast/runner.py`
  - end-to-end forecast/evaluation runner
- `src/gateway/src/gateway/forecast/storage_adapter.py`
  - loads forecast input windows from storage
- `src/gateway/src/gateway/forecast/telemetry_adjustments.py`
  - calibration and smoothing on forecast input paths
- `src/ml/src/forecasting/`
  - feature extraction, filtering, event detection, KNN forecasting, evaluation

### Synthetic pod

- `src/synthetic_pod/pod2_sim.py`
  - synthetic pod process entrypoint
- `src/synthetic_pod/sim/`
  - generator, weather, scheduling, zone, and fault logic

## 22. Source-of-Truth Principles

The system tries to keep these boundaries clear:

- firmware produces telemetry
- gateway ingests and persists telemetry
- forecasting reads persisted telemetry and writes forecast artifacts
- dashboard reads stored state and renders it

The dashboard is not the source of telemetry truth.

The forecasting layer is not the source of live sensor truth.

SQLite is the primary live source of truth for the running software stack.

## 23. Limitations

Important limits of the current implementation:

- not a certified compliance system
- not a BLE mesh network
- not a packet-capture/sniffer system
- not an industrial-grade calibration-management platform
- not a heavy ML platform with retraining orchestration

The current implementation is best described as:

- a clean, testable, end-to-end student system for environmental monitoring, link-quality tracking, short-horizon forecasting, and operator-facing dashboarding

## 24. Documentation Policy After Cleanup

This README is the primary maintained technical document for the repository.

The only extra README intentionally retained after cleanup is:

- `src/gateway/README.md`

That file remains only because `src/gateway/pyproject.toml` references it for packaging metadata. It should be treated as a minimal packaging placeholder, not as a second full technical manual.

## Forecast Evaluation and Latency Results

Methodology:
- Evaluation dataset: the SQLite database was scanned first and the day with the longest shared two-pod recording span was selected. The chosen day was `2026-03-29`, with `529` shared one-minute timestamps and `11.633` hours of overlap between pods `01` and `02`.
- Forecast setup: `180` minutes of history, `30` minutes of forecast horizon, and a `10`-minute evaluation cadence across the selected day.
- Scenario definitions: `stable` means the implemented event detector did not flag a recent disturbance; `disturbed` means the detector did flag a recent disturbance.
- Baseline definition: persistence, using the latest observed input-window value carried forward over the full next `30` minutes for both temperature and RH.
- Latency setup: `50` sequential synthetic TCP samples through the real gateway TCP ingester, SQLite write path, and dashboard JSON route.

| Scenario | Windows Evaluated | Temp MAE | Temp RMSE | RH MAE | RH RMSE | Notes |
|----------|-------------------|----------|-----------|--------|---------|-------|
| Stable | 67 | 0.300 C | 0.391 C | 2.046 % | 3.062 % | 57 windows used `analogue_knn`; 10 used `fallback_persistence` |
| Disturbed | 9 | 0.442 C | 0.503 C | 2.840 % | 3.446 % | All 9 windows were flagged by the event detector and used `analogue_knn` |
| Overall | 76 | 0.316 C | 0.405 C | 2.140 % | 3.110 % | Best shared day `2026-03-29`; both pods included |

| Scenario | Method | Temp MAE | Temp RMSE | RH MAE | RH RMSE | Better Than Persistence? |
|----------|--------|----------|-----------|--------|---------|--------------------------|
| Stable | Implemented forecast | 0.300 C | 0.391 C | 2.046 % | 3.062 % | No |
| Stable | Persistence | 0.152 C | 0.235 C | 0.876 % | 1.541 % | Reference |
| Disturbed | Implemented forecast | 0.442 C | 0.503 C | 2.840 % | 3.446 % | No |
| Disturbed | Persistence | 0.196 C | 0.261 C | 0.945 % | 1.381 % | Reference |
| Overall | Implemented forecast | 0.316 C | 0.405 C | 2.140 % | 3.110 % | No |
| Overall | Persistence | 0.157 C | 0.238 C | 0.884 % | 1.523 % | Reference |

| Metric | Value | Units | Sample Count | Notes |
|--------|-------|-------|--------------|-------|
| Median sample creation to gateway acceptance | 0.981 | ms | 50 | Measured from the synthetic TCP probe to gateway acceptance logging |
| Median sample creation to database availability | 18.141 | ms | 50 | First time the sample row became visible in SQLite |
| Median sample creation to dashboard/API visibility | 48.142 | ms | 50 | Measured via `/api/pods/<pod_id>/latest` |
| Median gateway acceptance to database visibility | 18.010 | ms | 50 | Extra breakdown of the storage stage only |
| Worst observed end-to-end latency | 88.670 | ms | 50 | Maximum `t3 - t0` across the full latency run |

Interpretation:
- Stable conditions were easier to forecast than disturbed conditions on the selected day, with both temperature and RH errors increasing when the event detector flagged abrupt recent change.
- On this dataset, the implemented forecasting method did **not** beat a simple persistence baseline overall or within either scenario group. The current prototype therefore behaved more like a disturbance-aware forecasting framework than a numerically stronger next-step predictor on this particular shared day.
- The measured latency was still strong for a warehouse monitoring prototype: the median path to dashboard/API visibility stayed below `50 ms` and the worst observed end-to-end latency stayed below `90 ms`. That is comfortably fast for environmental monitoring and alerting, even though it is not a hard real-time control result.
- Dew point was also evaluated as a derived quantity and followed the same trend: overall dew-point error was `0.491 C` MAE and `0.657 C` RMSE, with disturbed windows again worse than stable windows.

Reproducibility:
- Commands run:
  - `& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' -m unittest src\dashboard\tests\test_routes_smoke.py`
  - `& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\src\scripts\run_forecast_evaluation.py'`
  - `& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\src\scripts\run_latency_evaluation.py'`
- Scripts used:
  - `src/scripts/run_forecast_evaluation.py`
  - `src/scripts/run_latency_evaluation.py`
- Output files:
  - `evaluation/results/forecast_metrics.csv`
  - `evaluation/results/forecast_metrics.json`
  - `evaluation/results/baseline_comparison.csv`
  - `evaluation/results/baseline_comparison.json`
  - `evaluation/results/latency_records.csv`
  - `evaluation/results/latency_summary.json`
  - `evaluation/README_EVALUATION_NOTES.md`
