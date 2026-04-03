# Warehouse Spice Risk

Warehouse Spice Risk is an IoT warehouse-monitoring prototype for spice-stock preservation. The implemented stack combines:

- a physical BLE hardware pod based on an Adafruit Feather nRF52840 and SHT45 temperature/RH sensing
- a synthetic TCP pod used for multi-zone testing and fault injection
- a Python gateway that ingests both sources concurrently
- SQLite as the primary live store
- a 3-hour input / 30-minute horizon forecasting pipeline
- a Flask dashboard for overview, pod detail, alerts, prediction, health, and review views

This README is the main technical entry point for the GitHub repository.

## Repository Layout

Main code:

- `firmware/circuitpython-pod/`
- `gateway/src/gateway/`
- `ml/src/forecasting/`
- `dashboard/app/`
- `synthetic_pod/`
- `scripts/`
- `evaluation/`

Main data paths:

- SQLite live database: `data/db/telemetry.sqlite`
- Evaluation outputs: `evaluation/results/`

## Current Architecture

End-to-end runtime flow:

1. The physical pod emits BLE telemetry with `pod_id`, `seq`, `ts_uptime_s`, `temp_c`, `rh_pct`, and `flags`.
2. The synthetic pod emits TCP/JSON telemetry and supports replay/fault injection.
3. The gateway validates samples, handles gaps/duplicates/resends, tracks link quality, and stores accepted telemetry in SQLite.
4. The forecasting runner reads the most recent 3-hour window and produces 30-minute forecasts.
5. The dashboard reads stored telemetry and forecast outputs from SQLite and renders the operator-facing views.

Important implementation notes:

- SQLite is the primary live source of truth.
- The forecasting system is continuous-value forecasting, not classification.
- The current implementation does **not** implement BLE mesh networking.

## Quick Start

Create the local environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .\gateway
.\.venv\Scripts\python.exe -m pip install -r .\dashboard\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\synthetic_pod\requirements.txt
```

Run the main pieces:

```powershell
.\scripts\run_gateway_multi.ps1
.\scripts\run_pod2.ps1
.\scripts\run_dashboard.ps1
```

Useful helper tools:

```powershell
.\.venv\Scripts\python.exe .\scripts\pod_ble_monitor.py --list-only --scan-timeout 8
.\.venv\Scripts\python.exe .\scripts\pod_serial_monitor.py --port COM6
```

## Forecast Evaluation and Latency Results

Methodology:

- The SQLite database was analysed first to find the longest shared two-pod recording window.
- The selected evaluation day was `2026-03-29`, which had `529` shared one-minute timestamps and `11.633` hours of overlap between pods `01` and `02`.
- Forecast evaluation used the implemented `180`-minute history window, `30`-minute horizon, and a `10`-minute backtest cadence.
- `Stable` windows are windows where the implemented event detector did not flag a recent disturbance.
- `Disturbed` windows are windows where the event detector did flag a recent disturbance.
- The baseline is persistence: the latest observed temperature and RH values are held constant for the full next `30` minutes.
- The backtest was leakage-safe: the analogue case base started empty and only grew from earlier evaluated windows.
- Latency was measured over `50` live synthetic TCP samples through the real gateway TCP ingester, SQLite write path, and dashboard JSON route.

| Scenario | Windows Evaluated | Temp MAE | Temp RMSE | RH MAE | RH RMSE | Notes |
|----------|-------------------|----------|-----------|--------|---------|-------|
| Stable | 67 | 0.300 C | 0.391 C | 2.046 % | 3.062 % | 57 windows used `analogue_knn`; 10 used `fallback_persistence` |
| Disturbed | 9 | 0.442 C | 0.503 C | 2.840 % | 3.446 % | All disturbed windows were flagged by the existing event detector |
| Overall | 76 | 0.316 C | 0.405 C | 2.140 % | 3.110 % | Both pods included on the best shared day |

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
| Median gateway acceptance to database visibility | 18.010 | ms | 50 | Storage-stage breakdown only |
| Worst observed end-to-end latency | 88.670 | ms | 50 | Maximum `t3 - t0` across the latency run |

Interpretation:

- Stable conditions were easier to forecast than disturbed conditions on the chosen shared-data day.
- On this measured dataset, the implemented forecast did **not** outperform simple persistence overall or within either scenario group.
- The latency results were still strong for a warehouse-monitoring prototype: median dashboard visibility stayed below `50 ms`, and the worst observed end-to-end latency stayed below `90 ms`.

## Reproducibility

Commands used:

```powershell
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' -m unittest dashboard\tests\test_routes_smoke.py
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\scripts\run_forecast_evaluation.py'
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\scripts\run_latency_evaluation.py'
```

Main scripts:

- `scripts/run_forecast_evaluation.py`
- `scripts/run_latency_evaluation.py`

Main outputs:

- `evaluation/results/forecast_metrics.csv`
- `evaluation/results/forecast_metrics.json`
- `evaluation/results/baseline_comparison.csv`
- `evaluation/results/baseline_comparison.json`
- `evaluation/results/latency_records.csv`
- `evaluation/results/latency_summary.json`
- `evaluation/README_EVALUATION_NOTES.md`
