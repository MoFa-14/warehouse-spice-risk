# Evaluation Notes

## Dataset Selection

- SQLite database used: `src/data/db/telemetry.sqlite`
- Best shared two-pod day selected automatically from SQLite coverage:
  - `2026-03-29`
  - shared one-minute overlap: `529` minutes
  - shared overlap span: `11.633` hours
  - shared interval: `2026-03-29T12:21:00Z` to `2026-03-29T23:59:00Z`
- Pod sample counts on the selected day:
  - pod `01`: `531`
  - pod `02`: `571`

## Forecast Evaluation Method

- Forecast history window: `180` minutes
- Forecast horizon: `30` minutes
- Evaluation cadence: every `10` minutes on the selected day
- Evaluated pod-window count: `76`
  - stable windows: `67`
  - disturbed windows: `9`
- Scenario definition:
  - stable = the implemented event detector did not flag a recent disturbance
  - disturbed = the implemented event detector flagged a recent disturbance
- Forecast under test:
  - the implemented baseline trajectory produced by the existing forecasting pipeline
  - event-persist remains available in code but was not used as the main scored method for the one-to-one baseline comparison
- Baseline:
  - persistence forecast using the latest observed input-window value carried over the full next `30` minutes for both temperature and RH
- Leakage control:
  - the backtest was run sequentially in timestamp order
  - the analogue case base started empty and was allowed to grow only from earlier evaluated windows
  - no future windows were made available to earlier forecasts
- Observed model source usage during the backtest:
  - `66` windows used `analogue_knn`
  - `10` windows used `fallback_persistence`

## Latency Method

- Latency path measured:
  - synthetic TCP probe -> gateway TCP ingester -> router -> SQLite -> dashboard JSON route
- Latency sample count: `50`
- Pod id used for the latency probe: `99`
- Timing points:
  - `t0` = source send time recorded by the probe
  - `t1` = gateway acceptance time from the opt-in timing log
  - `t2` = first time the SQLite row became visible
  - `t3` = first time the dashboard JSON route exposed the sample
- Dashboard visibility route:
  - `/api/pods/<pod_id>/latest`
- Temporary evaluation database:
  - `evaluation/results/latency_telemetry.sqlite`

## Commands Run

```powershell
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' -m unittest src\dashboard\tests\test_routes_smoke.py
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\src\scripts\run_forecast_evaluation.py'
& 'C:\Users\TERA MAX\Desktop\DSP\src\.venv\Scripts\python.exe' '.\src\scripts\run_latency_evaluation.py'
```

## Output Files

- `evaluation/results/forecast_metrics.csv`
- `evaluation/results/forecast_metrics.json`
- `evaluation/results/baseline_comparison.csv`
- `evaluation/results/baseline_comparison.json`
- `evaluation/results/latency_records.csv`
- `evaluation/results/latency_summary.json`
- `evaluation/results/latency_gateway_events.jsonl`

## Key Observations

- Stable windows were easier than disturbed windows:
  - stable temperature MAE: `0.300 C`
  - disturbed temperature MAE: `0.442 C`
  - stable RH MAE: `2.046 %`
  - disturbed RH MAE: `2.840 %`
- On the selected day, the implemented forecast did **not** beat simple persistence:
  - overall implemented temp MAE: `0.316 C`
  - overall persistence temp MAE: `0.157 C`
  - overall implemented RH MAE: `2.140 %`
  - overall persistence RH MAE: `0.884 %`
- Latency on the synthetic TCP path was comfortably sub-second:
  - median gateway acceptance latency: `0.981 ms`
  - median database visibility latency: `18.141 ms`
  - median dashboard visibility latency: `48.142 ms`
  - worst observed end-to-end latency: `88.670 ms`

## Limitations

- The forecast evaluation day was selected for maximum shared pod overlap, but the analogue case base still began empty and had to grow online.
- The latency measurement used the synthetic TCP ingestion path and a temporary evaluation database, not the physical BLE pod path.
- Dashboard visibility was measured through the new JSON route that exposes the same latest-reading data used by the HTML pages.
