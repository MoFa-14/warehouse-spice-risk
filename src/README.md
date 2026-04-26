# Warehouse Spice Risk

Warehouse Spice Risk is an end-to-end environmental monitoring and forecasting
prototype for warehouse spice preservation. The repository combines hardware
firmware, a multi-source gateway, a short-horizon forecasting pipeline, a
SQLite-backed storage layer, a Flask dashboard, and a synthetic pod cluster
used to exercise the system beyond the single physical sensor pod.

The project is designed as a full-stack prototype rather than an isolated
forecasting notebook or a simple sensor logger. Its purpose is to demonstrate
how warehouse environmental data can be:

1. sensed close to storage conditions,
2. transported through real and synthetic communication paths,
3. validated and persisted centrally,
4. transformed into forecast-ready history windows,
5. forecast over a short operational horizon, and
6. interpreted visually through a monitoring and review interface.

The codebase therefore shows not only prediction logic, but the engineering
infrastructure required to make prediction meaningful in a live monitoring
system.

## Table of Contents

- [Project Purpose](#project-purpose)
- [Prototype Scope](#prototype-scope)
- [Repository Layout](#repository-layout)
- [Full System Architecture](#full-system-architecture)
- [Hardware and Firmware](#hardware-and-firmware)
- [Communication and Ingestion](#communication-and-ingestion)
- [Storage Design](#storage-design)
- [Forecasting System](#forecasting-system)
- [Recent Forecasting Corrections and Findings](#recent-forecasting-corrections-and-findings)
- [Dashboard Behaviour](#dashboard-behaviour)
- [Testing](#testing)
- [Evaluation and Interpretation Guidance](#evaluation-and-interpretation-guidance)
- [Running the System](#running-the-system)
- [Current Limitations and Future Work](#current-limitations-and-future-work)
- [Subsystem Documentation](#subsystem-documentation)

## Project Purpose

Spices are sensitive to environmental conditions during storage. Relative
humidity influences moisture exchange, caking, mould risk, and quality
degradation. Temperature affects both the absolute moisture capacity of air and
the rate at which environmental change occurs. Dew point provides a useful
psychrometric interpretation of these quantities by describing the moisture
state that would lead to condensation if local temperatures fall.

In a warehouse setting, logging only the current reading is often insufficient.
Operators also need to understand:

- whether values are stable or drifting,
- whether recent behaviour looks like a disturbance or normal variability,
- whether communication is healthy,
- whether missing readings reflect a real outage or a normal interval, and
- whether a short-horizon forecast provides useful information beyond simply
  holding the latest reading constant.

This project addresses those needs by combining:

- a **physical sensing pod** that transmits real readings,
- a **synthetic pod cluster** that simulates additional warehouse zones,
- a **gateway** that ingests, validates, stores, and analyses telemetry,
- a **forecasting pipeline** that predicts the next 30 minutes from the last
  3 hours of history, and
- a **dashboard** that presents live conditions, history, forecast scenarios,
  and completed forecast evaluation.

The prototype demonstrates the feasibility and structure of a warehouse
monitoring system rather than claiming production-ready operational accuracy.

## Prototype Scope

The repository currently demonstrates the following operational picture:

- Pod `01` is the physical hardware pod.
- Pods `02` to `10` are synthetic pods generated in software.
- The gateway ingests physical and synthetic telemetry concurrently.
- SQLite acts as the central live-system database.
- Forecasts are produced on a rolling cadence from stored telemetry.
- Completed forecasts are evaluated once the next 30 minutes of realised data
  become available.
- The dashboard displays pod summaries, history charts, alert context,
  prediction scenarios, persistence comparison, and a dedicated historical
  forecasting analysis section for Pod `01`.

The prototype is intentionally narrow in horizon and feature set:

- history window: **3 hours**
- resampling interval: **1 minute**
- forecast horizon: **30 minutes**

This fixed structure keeps the forecasting task interpretable and keeps
evaluation windows comparable.

## Repository Layout

The most important top-level folders are:

- `firmware/`
  - CircuitPython firmware for the physical pod and supporting deployment files.
- `gateway/`
  - Gateway-side ingestion, routing, validation, storage, preprocessing, and
    forecasting orchestration.
- `ml/`
  - Forecasting models and forecasting-specific data structures.
- `dashboard/`
  - Flask dashboard, data-access helpers, services, templates, and tests.
- `synthetic_pod/`
  - Software pods used to simulate additional zones and communication faults.
- `scripts/`
  - Convenience entry points for the integrated demonstration workflow.
- `data/`, `raw/`, `processed/`, `evaluation/`
  - Runtime and analysis outputs created by the system.

Each major subsystem now also contains its own local `README.md` with a
folder-level explanation and file inventory.

## Full System Architecture

### 1. Sensing Layer

The sensing layer consists of two pod categories.

#### Physical Pod

The physical pod is built around:

- an Adafruit Feather nRF52840 board,
- an SHT45 temperature and relative humidity sensor,
- CircuitPython firmware,
- a custom BLE service carrying telemetry, status, and simple control writes.

The physical pod is responsible for:

- sampling the physical environment,
- maintaining a monotonic sequence number,
- caching recent samples in a ring buffer,
- exposing a status summary,
- advertising a gateway-readable BLE service,
- and replaying the newest sample immediately on connection.

#### Synthetic Pods

The synthetic pods are not intended to replace the hardware pod. Their role is
to demonstrate system behaviour under a larger pod count and under controlled
fault scenarios, including:

- different baseline zone profiles,
- burst loss,
- intermittent disconnection,
- schedule-driven behaviour,
- and event-like disturbances.

The synthetic pod cluster produces telemetry that follows the same logical
schema as the physical pod even though it uses a different transport.

### 2. Communication Layer

The communication layer has two live ingestion paths.

#### BLE Path

The physical pod broadcasts a custom BLE service. The gateway:

- scans for matching services or configured addresses,
- connects,
- decodes telemetry notifications,
- records connection quality and reconnection behaviour,
- and forwards normalised records into the common gateway queue.

#### Synthetic TCP Path

Synthetic pods connect to a local TCP listener and send newline-delimited JSON
telemetry. The gateway:

- accepts many synthetic connections concurrently,
- decodes each telemetry line,
- records corrupt-line events when parsing fails,
- and forwards the same normalised record type used by the BLE path.

The key architectural choice is that downstream stages do **not** care whether
the sample arrived from BLE or TCP. Both are converted to a shared internal
record format.

### 3. Storage Layer

SQLite is the central storage engine for the integrated runtime system.

It stores:

- raw telemetry samples,
- link-quality snapshots,
- gateway events,
- analogue case-base rows,
- forecast scenario rows,
- completed evaluation rows,
- and any supporting metadata required by the forecasting loop.

Older and auxiliary CSV-based flows still exist in places for export and
compatibility, but SQLite is the main source of truth for the current
repository.

### 4. Application and Control Layer

The application and control layer contains three main roles.

#### Gateway

The gateway is responsible for:

- ingesting BLE and TCP telemetry,
- decoding and validating records,
- routing multi-pod data into storage,
- computing link diagnostics,
- generating forecasts,
- evaluating completed forecasts,
- updating the case base,
- and applying forecast calibration or filtering rules.

#### Forecasting Package

The forecasting package is a domain-specific layer that:

- represents forecast-ready time series windows,
- extracts features,
- detects recent events,
- builds baseline and event-persist scenarios,
- compares against persistence,
- and computes post-hoc forecast evaluation metrics.

#### Dashboard

The dashboard is responsible for:

- latest pod state presentation,
- history visualisation,
- threshold interpretation,
- forecast scenario display,
- persistence-comparison plotting,
- and historical forecast inspection using stored forecast/evaluation products.

## Hardware and Firmware

### Physical Pod Hardware

The physical pod is located under `firmware/circuitpython-pod/`.

Important files include:

- `code.py`
  - top-level firmware loop executed by CircuitPython.
- `ble_service.py`
  - BLE service definition and event-oriented wrapper around the radio.
- `sensors.py`
  - SHT45 access and read-failure recovery.
- `config.py`
  - pod identity, UUIDs, intervals, limits, and firmware constants.
- `ring_buffer.py`
  - recent-sample cache used for replay after reconnects.
- `status.py`
  - compact status payload generation.
- `deploy_to_circuitpy.ps1`
  - deployment helper for copying files to the board.
- `verify_deploy.ps1`
  - deployment verification helper.

### Why These Hardware Choices Matter

The Feather nRF52840 provides a practical BLE-capable microcontroller platform
for a student prototype, while the SHT45 offers dedicated temperature and
relative humidity sensing with a clear digital interface. Together they allow
the project to demonstrate real wireless environmental telemetry without
introducing unnecessary hardware complexity.

### Firmware Behaviour Step by Step

At a high level, the firmware performs the following sequence:

1. boot and load configuration,
2. initialise the SHT45 sensor wrapper,
3. create the BLE peripheral and custom service,
4. take an immediate first sample before advertising,
5. advertise the service,
6. periodically sample on the configured interval,
7. publish the latest sample if a central device is connected,
8. update the status characteristic,
9. accept a small control-command grammar,
10. replay the newest sample on connection so the gateway starts with a known
    recent reading.

This design keeps the pod intentionally simple. It does not perform on-device
forecasting or heavy processing. Its role is to create dependable environmental
telemetry at the edge.

## Communication and Ingestion

### BLE Ingestion Path

Relevant gateway files:

- `gateway/src/gateway/ble/client.py`
- `gateway/src/gateway/ble/gatt.py`
- `gateway/src/gateway/ble/scanner.py`
- `gateway/src/gateway/ingesters/ble_ingester.py`
- `gateway/src/gateway/protocol/decoder.py`
- `gateway/src/gateway/protocol/validation.py`

The BLE path proceeds as follows:

1. discover matching pods using name prefix, service UUID, or explicit address,
2. connect and verify the expected GATT profile,
3. decode status and telemetry payloads,
4. validate fields without discarding the record context,
5. attach connection-quality information such as received signal strength
   indication,
6. register resend and reconnect support,
7. convert the sample into the gateway's shared telemetry record,
8. place the record onto the multi-pod queue.

### Synthetic Pod Ingestion Path

Relevant gateway files:

- `gateway/src/gateway/ingesters/tcp_ingester.py`
- `gateway/src/gateway/protocol/ndjson.py`

The TCP path proceeds as follows:

1. accept a client connection from a synthetic pod,
2. read one newline-delimited JSON message at a time,
3. ignore unexpected command payloads,
4. decode telemetry into the same typed record used by the BLE path,
5. record corrupt or malformed messages as diagnostics,
6. route the sample into the shared queue.

### Why Validation, Sequence Handling, and Diagnostics Matter

Warehouse monitoring data becomes much less useful if communication issues are
silently hidden. The gateway therefore tracks:

- missing values,
- out-of-range values,
- sensor error flags,
- duplicates,
- reconnect counts,
- missing-rate estimates,
- sequence resets,
- and corrupt packets or lines.

These mechanisms matter because forecasting and dashboard interpretation depend
on understanding whether a surprising pattern reflects the real environment or a
communication or sensor problem.

## Storage Design

### Why SQLite Is Used

SQLite is appropriate here because the project is:

- single-host during the live demo,
- write-heavy but not high-throughput at industrial scale,
- dependent on structured history for forecasting,
- and easier to inspect and move than a server-based database.

It provides:

- strong enough transactional behaviour for the gateway,
- direct testability,
- straightforward schema evolution,
- and simple integration with pandas and dashboard readers.

### What Is Stored

The database persists several categories of information.

#### Raw Telemetry

The `samples_raw` table stores accepted telemetry rows with:

- gateway timestamp,
- pod identifier,
- session identifier,
- sequence number,
- pod uptime timestamp,
- temperature,
- relative humidity,
- raw flags,
- received signal strength indication when applicable,
- quality flags,
- and data source.

#### Link Quality

The `link_quality` table stores snapshots of communication health, including:

- connected state,
- last received signal strength indication,
- total received count,
- total missing count,
- total duplicates,
- disconnect and reconnect counters,
- and missing-rate summaries.

#### Forecasting Artefacts

Forecast-related persistence includes:

- forecast scenario rows,
- completed evaluation rows,
- and case-base records used by the analogue model.

This separation matters because forecasts are generated before outcomes occur,
while evaluation rows are generated later when the actual future becomes known.

### Raw and Processed Data Relationship

Raw samples are the closest record of what the gateway accepted. Processed CSV
artefacts and dashboard-adjusted views exist for analysis and presentation, but
the forecasting loop relies primarily on the stored telemetry history and the
forecast-specific storage tables.

## Forecasting System

The forecasting design is implemented across:

- `ml/src/forecasting/`
- `gateway/src/gateway/forecast/`
- `dashboard/app/services/prediction_service.py`
- `dashboard/app/services/forecast_test_service.py`

### Forecasting Task Definition

The current forecast task is deliberately fixed:

- **input history**: previous 3 hours
- **time grid**: 1-minute samples
- **prediction horizon**: next 30 minutes

This design removes ambiguity about what each forecast means. Every generated
forecast, every evaluation row, and every historical case uses the same basic
window geometry.

### History Loading and Resampling

The gateway forecast runner reads the most recent telemetry window for a pod and
converts it into a regular 1-minute grid. This stage matters because:

- raw telemetry can contain communication gaps,
- physical and synthetic sources can arrive with different timing characteristics,
- and feature extraction should operate on a consistent temporal structure.

The storage adapter acts as the bridge between database history and the
forecasting package's `TimeSeriesPoint` representation.

### Feature Extraction

Feature extraction is implemented in `ml/src/forecasting/features.py`.

The feature vector includes:

- latest temperature, relative humidity, and dew point,
- short-tail slopes over 15, 30, and 60 minute windows,
- recent variability measures,
- recent minimum and maximum values,
- and hour-of-day information encoded for similarity matching.

These features exist because the analogue model does not compare raw
minute-by-minute windows directly. Instead it compares a compact summary of the
current environmental state and recent trajectory.

### Event Detection

Event detection is implemented in `ml/src/forecasting/event_detection.py`.

Its purpose is to answer whether the latest part of the history window looks
like a normal continuation or a recent disturbance. The logic examines:

- recent temperature change,
- recent relative humidity change,
- recent dew point change,
- robust threshold estimates,
- and the latest breach segment.

The result is used in two ways:

1. to decide whether a separate event-persist scenario should be generated,
2. to help build a baseline-safe history window that does not let a short event
   dominate the analogue comparison more than intended.

### Baseline Forecast

The baseline forecast is the default forecast scenario shown in the dashboard.

It is produced by the analogue / nearest-neighbour forecaster in
`ml/src/forecasting/knn_forecaster.py`.

The process is:

1. construct the current feature vector,
2. load candidate historical cases from the case base,
3. compute similarity distance,
4. apply relative-humidity and dew-point regime compatibility checks,
5. apply a small recency penalty so newer cases are slightly preferred,
6. aggregate neighbour futures,
7. re-anchor neighbour futures relative to the current live anchor.

The re-anchoring step is essential. The model now treats neighbour futures as
offset patterns from each neighbour's own anchor rather than copying neighbour
absolute values directly.

### Event-Persist Scenario

The event-persist scenario is a deliberately interpretable alternative scenario.

It asks: if the recent disturbance continues for a while, what might happen over
the next 30 minutes?

The event-persist logic is simpler than the baseline model. It extrapolates
recent behaviour using deterministic bounds and damping. For relative humidity,
the continuation is no longer an unlimited straight-line extension of the recent
slope; the logic now uses damping and caps total drift across the horizon.

The event-persist path is intended as a comparative scenario, not as a claim
that the disturbance will definitely continue in that exact form.

### Persistence Baseline

The system also constructs a persistence baseline in which:

- the latest observed temperature is held constant,
- the latest observed relative humidity is held constant,
- and dew point is recomputed from those constant values.

This is not the main user-facing forecast scenario. It is the reference
comparator used to judge whether the model actually adds information.

### Dew Point Derivation

Dew point is **derived**, not forecasted directly as a separate target.

The repository calculates dew point from forecasted temperature and forecasted
relative humidity using a Magnus-style approximation. This keeps psychrometric
relationships internally consistent and makes dew point errors easier to trace:

- if dew point is wrong, the root cause usually lies in temperature forecast
  error, relative humidity forecast error, or both.

### Forecast Storage

Forecast rows are persisted so the dashboard does not recompute them on demand.

This design has several advantages:

- the dashboard remains a read-oriented application,
- forecasts can later be evaluated against what actually happened,
- historical forecast behaviour can be inspected even after the live moment has
  passed,
- and different dashboard pages can render the same stored forecast consistently.

### Forecast Evaluation

Evaluation is performed after the corresponding future 30-minute outcome window
has become available. The evaluator computes:

- mean absolute error,
- root mean square error,
- bias,
- completeness checks,
- and flags such as `large_error`.

Persistence-comparison metrics are also stored so the dashboard can plot
per-window advantage versus persistence.

## Recent Forecasting Corrections and Findings

### Misleading Improvement Chart Logic

An earlier version of the dashboard improvement chart used the earliest stored
root mean square error as its reference point. That was misleading because the
first-ever error for a pod is not a meaningful baseline, and a very small early
error could make later percentage values look wildly negative.

The corrected logic now compares each completed forecast window directly
against persistence. The chart therefore represents a real per-window model
advantage or disadvantage rather than an expanding historical artefact.

### Analogue Anchoring Problem

The original analogue forecast could drift immediately toward a neighbour's
absolute regime. That produced visible jumps away from the latest observed
reading even when the current environment had not moved that way yet.

The current implementation re-anchors neighbour futures to the live anchor by
forecasting relative offsets instead of copying raw neighbour levels.

### Wrong Historical Regime Matching

Relative humidity forecasts were especially sensitive to poor analogue matching.
When the case base was small, the model could accept historical cases from a
different humidity regime and then aggregate them into an unrealistic forecast.

The current targeted correction introduces:

- explicit relative-humidity compatibility checks,
- explicit dew-point compatibility checks,
- a small recency preference,
- and a fallback or blend toward persistence when analogue support is too weak.

These changes reduce bad-regime matches, but they do not eliminate the
underlying limitation that the case base is still relatively small.

### Limited Case-Base Size

Because the project learns from stored completed baseline windows, model quality
depends on how much representative historical telemetry has already been
observed and evaluated. This means the forecaster can still struggle when:

- a pod has only a small number of historical cases,
- the current environmental regime has few good analogues,
- or long-term seasonal context is not well represented in stored history.

### Historical Stored Forecast Rows

The dashboard's historical forecast analysis can display stored forecast rows
that were generated before a model correction was implemented. That means:

- the historical review tools are faithfully showing what the system stored at
  the time,
- but some old rows may reflect older forecast behaviour rather than the latest
  corrected logic.

This is an important interpretive point, especially for the `Pod 1 Forecasting
Test` card.

### Calibration Eligibility

The forecast pipeline filters which completed evaluations can influence later
calibration. Large-error or incomplete windows should not be allowed to bias the
calibration process as if they were representative good examples.

### What Is Fixed and What Remains a Limitation

Fixed or improved:

- persistence-based improvement chart,
- analogue re-anchoring,
- stronger relative-humidity regime protection,
- damped event-persist relative-humidity behaviour,
- clearer historical forecasting test tools.

Still limited:

- relatively small case base,
- limited seasonal awareness,
- old stored forecast rows may remain visible until regenerated,
- forecast skill still requires broader validation over more real operating
  history.

## Dashboard Behaviour

The dashboard lives under `dashboard/app/` and is organised into routes,
data-access helpers, services, templates, and static assets.

### Overview and Pod Pages

The overview page summarises the currently known pods and their latest state.
The individual pod page adds:

- the latest sample state card,
- threshold interpretation,
- history plots for temperature, relative humidity, and dew point,
- and the most recent stored forecast context for that pod.

### Prediction Page

The prediction page focuses on stored forecast products. It shows:

- baseline forecast scenario,
- event-persist scenario when available,
- persistence-aware evaluation context,
- forecast summary cards,
- dew point forecast,
- and the persistence-comparison chart.

### Review Page

The review page interprets longer windows of stored telemetry and connection
history to support system health review rather than immediate forecast viewing.

### `Pod 1 Forecasting Test`

The `Pod 1 Forecasting Test` card is a historical analysis tool rather than a
live forecast.

It:

- selects the best long continuous Pod `01` session from stored data,
- identifies completed forecast attempts within that session,
- plots the session overview,
- and allows detailed comparison of one historical forecast attempt against the
  realised next 30 minutes and the persistence baseline.

This section exists because current live forecasts do not by themselves show how
the system behaved on a historically completed window.

### How Stored Forecasts and Evaluations Are Read

The dashboard does not build forecasts itself. Instead it reads:

- stored forecast rows,
- stored evaluation rows,
- raw telemetry for context or reconstruction,
- and local configuration/adjustment files where needed.

This keeps forecasting responsibilities in the gateway and forecasting package
while keeping the dashboard responsible for interpretation and presentation.

## Testing

The repository includes subsystem-focused automated tests.

### Forecasting / Machine Learning Tests

Located under `ml/tests/`, these tests cover:

- feature extraction,
- event detection,
- baseline window filtering,
- analogue forecasting,
- scenario generation,
- evaluator behaviour.

Recent forecasting-fix tests specifically check:

- re-anchoring behaviour,
- relative-humidity regime gating,
- recency preference,
- weak-support fallback,
- event-persist damping behaviour.

### Gateway Tests

Located under `gateway/tests/`, these tests cover:

- protocol decoding,
- validation,
- TCP ingestion,
- multi-source routing,
- SQLite writing and reading,
- preprocessing and resampling,
- forecast runner orchestration,
- process locking,
- command-line behaviour.

These tests are meant to catch failures such as:

- duplicate handling regressions,
- schema mismatches,
- broken queue/storage flow,
- bad resampling behaviour,
- forecast-loop collisions,
- or protocol decoding failures.

### Dashboard Tests

Located under `dashboard/tests/`, these tests cover:

- latest-reading selection,
- timeseries path building,
- prediction-page context creation,
- threshold behaviour,
- historical forecast test-card logic,
- route rendering,
- and general smoke behaviour of the Flask app.

### What Automated Testing Proves and Does Not Prove

Automated tests prove that:

- code paths execute as expected under selected scenarios,
- critical transformations preserve intended structure,
- and recent bug fixes remain guarded against regression.

Automated tests do **not** prove that:

- forecast skill is broadly sufficient in all warehouse regimes,
- synthetic data perfectly reflects real warehouse microclimates,
- or a small historical case base is already representative enough for
  operational deployment.

## Evaluation and Interpretation Guidance

### Baseline vs Event-Persist vs Persistence

- **Baseline forecast**:
  the model's best normal forecast based on recent history and stored analogue
  cases.
- **Event-persist forecast**:
  a "what if the current disturbance continues?" alternative scenario.
- **Persistence baseline**:
  the trivial forecast that the latest conditions do not change.

### Interpreting Positive and Negative Persistence Comparison Values

The corrected persistence-comparison chart uses per-window model advantage over
persistence. Interpreting it is straightforward:

- positive value: the model beat persistence on that completed window,
- zero: the model and persistence were equivalent,
- negative value: persistence performed better.

### Interpreting the Historical Test Card

The historical test card should be read as a comparison between:

- the recent history window available at forecast time,
- the stored forecast the system produced then,
- the persistence baseline,
- and the actual realised next 30 minutes.

If a historical row looks poor, it is important to consider whether it came
from:

- a small or badly matched case base,
- an older stored model version,
- or a deliberately pessimistic event-persist scenario.

## Running the System

Typical integrated workflow:

```powershell
.\scripts\run_gateway_multi.ps1
.\scripts\run_synthetic_pods.ps1
.\scripts\run_dashboard.ps1
& '.\.venv\Scripts\python.exe' '.\scripts\run_forecasting_auto.py'
```

Helpful operational scripts:

- `scripts/run_gateway_multi.ps1`
  - start the gateway in mixed physical-plus-synthetic mode.
- `scripts/run_synthetic_pods.ps1`
  - start synthetic pods `02` to `10`.
- `scripts/run_dashboard.ps1`
  - start the Flask dashboard.
- `scripts/run_forecasting_auto.py`
  - run the unified automatic forecasting loop.
- `scripts/run_forecast_evaluation.py`
  - offline forecast evaluation and reporting support.
- `scripts/pod_ble_monitor.py`
  - inspect pod discovery and BLE communication.
- `scripts/pod_serial_monitor.py`
  - support direct serial observation of the pod firmware.

## Current Limitations and Future Work

### Forecasting Limitations

- The analogue case base is still relatively small.
- Strong seasonal awareness is limited.
- Recency and regime protection help, but cannot create good analogues where
  history is sparse.
- Historical stored forecast rows may still reflect older model behaviour.
- Forecast quality still needs broader validation on larger real datasets.

### Synthetic Pod Limitations

- Synthetic pods are useful for scale and communication testing, but they are
  not ground truth for warehouse behaviour.
- Synthetic event patterns are controlled approximations, not true warehouse
  disturbances.

### Dashboard Historical Analysis Limitations

- The historical test card is highly useful for inspection, but it inherits the
  quality of the stored forecasts available for the chosen session.
- If the stored row predates a forecast fix, the card will faithfully display
  that older behaviour until forecasts are regenerated.

### Future Improvement Directions

- expand the case base with more representative real data,
- introduce stronger seasonality and regime awareness,
- improve recalibration governance with richer eligibility criteria,
- support explicit historical forecast regeneration workflows,
- validate forecast skill across longer real warehouse observation periods,
- and extend physical deployment beyond a single hardware pod.

## Subsystem Documentation

In addition to this root guide, the repository now includes detailed folder
documentation in local `README.md` files, including:

- `firmware/README.md`
- `firmware/circuitpython-pod/README.md`
- `gateway/README.md`
- `gateway/src/gateway/README.md`
- `gateway/src/gateway/ble/README.md`
- `gateway/src/gateway/ingesters/README.md`
- `gateway/src/gateway/protocol/README.md`
- `gateway/src/gateway/storage/README.md`
- `gateway/src/gateway/preprocess/README.md`
- `gateway/src/gateway/forecast/README.md`
- `gateway/src/gateway/multi/README.md`
- `gateway/src/gateway/link/README.md`
- `gateway/src/gateway/cli/README.md`
- `gateway/src/gateway/control/README.md`
- `gateway/src/gateway/utils/README.md`
- `dashboard/README.md`
- `dashboard/app/README.md`
- `dashboard/app/data_access/README.md`
- `dashboard/app/services/README.md`
- `dashboard/app/web/README.md`
- `ml/README.md`
- `ml/src/forecasting/README.md`
- `ml/tests/README.md`
- `gateway/tests/README.md`
- `dashboard/tests/README.md`
- `synthetic_pod/README.md`
- `synthetic_pod/sim/README.md`
- `synthetic_pod/tests/README.md`
- `scripts/README.md`

Those local documents describe the subsystem-specific responsibilities, key
files, design choices, and testing roles in more detail.
