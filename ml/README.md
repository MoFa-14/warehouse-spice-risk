# Prediction ML Logic: 30-Minute Forecasting

This module implements the warehouse pod "Prediction ML logic" as a forecasting pipeline for the next 30 minutes of:

- temperature (`temp_c`)
- relative humidity (`rh_pct`)
- dew point (`dew_point_c`, derived)

The forecaster always works on a fixed 3-hour input window resampled to 1-minute steps. It does not perform classification, deep learning, or seasonal binning.

## Goal

For each `pod_id`, the pipeline reads the latest 3 hours of telemetry and produces:

- `temp_forecast[t+1..t+30]`
- `rh_forecast[t+1..t+30]`
- `dew_point_forecast[t+1..t+30]`
- 25th and 75th percentile bands for temperature and RH
- event metadata:
  - `event_detected`
  - `event_type`
  - `event_reason`

## Event Detection And Two Scenarios

The first stage looks for unusual disturbances in the recent part of the 3-hour window.

- It computes 5-minute deltas for temperature and RH.
- It builds robust thresholds from the same 3-hour window using MAD-scaled dispersion.
- It also checks hard jump limits:
  - RH jump greater than `5%` in 5 minutes
  - temperature jump greater than `1.5 C` in 5 minutes
- A recent disturbance is flagged when the tail of the window shows consecutive threshold breaches or a hard jump.

Event labelling is intentionally transparent:

- `door_open_like`: RH movement dominates and dew point is rising
- `ventilation_issue_like`: temperature movement dominates while RH change stays smaller
- `unknown`: any disturbance that does not cleanly match the two simple heuristics

If an event is detected, the pipeline outputs two forecast scenarios:

1. `baseline`
   Uses an event-robust filtered version of the recent history so the forecast estimates the undisturbed baseline trend.
2. `event_persist`
   Assumes the disturbance continues. This implementation uses bounded continuation of the current 5-minute raw slope.

## Robust Baseline Filtering

The raw window is never overwritten. Baseline filtering only affects the forecast input.

- When an event is detected, the filter clips minute-to-minute deltas from the event start onward.
- The clip limits come from pre-event robust delta dispersion plus small minimum caps.
- The filtered series is then reconstructed and dew point is recomputed from filtered temperature and RH.

This makes the baseline forecast more robust to sharp door-opening or ventilation spikes without hiding the original data.

## Analogue / kNN Forecasting

The baseline forecast uses analogue matching once the case base has enough historical examples.

### Feature vector

At forecast time `t0`, the forecaster extracts:

- last values:
  - `temp_last`
  - `rh_last`
  - `dew_last`
- slopes:
  - `temp_slope_15`, `temp_slope_30`, `temp_slope_60`
  - `rh_slope_15`, `rh_slope_30`, `rh_slope_60`
  - `dew_slope_30`
- volatility:
  - `temp_std_30`, `temp_std_60`
  - `rh_std_30`, `rh_std_60`
- extremes:
  - `temp_min_60`, `temp_max_60`
  - `rh_min_60`, `rh_max_60`
- time-of-day cyclical features:
  - `hour_sin`
  - `hour_cos`

Season bins are intentionally not implemented yet.

### Similarity search

- The case base stores past feature vectors plus the realized next-30-minute trajectories.
- Features are normalized with running mean and standard deviation computed from the available case base.
- Distance is a weighted Euclidean score across the feature set.
- The forecaster picks the top-`k` nearest historical cases.

### Forecast aggregation and uncertainty

For each minute `h=1..30`:

- temperature forecast is the median of neighbour futures
- RH forecast is the median of neighbour futures
- the uncertainty band is the neighbour distribution's:
  - 25th percentile
  - 75th percentile

When the case base is still small, the module falls back to bounded slope persistence using the filtered baseline window. In that fallback mode, the band is derived from recent volatility instead of analogue spread.

## Online Learning

The case base grows online after each forecast horizon completes.

- After 30 minutes, the runner fetches the realized `t+1..t+30` actual trajectory.
- It computes:
  - MAE for temperature
  - RMSE for temperature
  - MAE for RH
  - RMSE for RH
- If the input and realized horizon both pass the missing-rate gate, the feature vector plus realized future are stored as a new analogue case.

This means the analogue database improves matching availability over time without heavy retraining.

## Storage

Preferred live input:

- `data/db/telemetry.sqlite`

Fallback input:

- `data/raw/pods/<pod_id>/YYYY-MM-DD.csv`

Forecast outputs are stored in:

- SQLite mode:
  - `forecasts` table
  - `evaluations` table
  - `case_base` table
- CSV-only mode:
  - `data/ml/forecasts.jsonl`
  - `data/ml/evaluations.jsonl`
  - `data/ml/case_base.jsonl`

## Running

Examples from the repository root:

```powershell
python -m gateway.cli.forecast_cli once --pod 01
python -m gateway.cli.forecast_cli run --pod 01 --every-minutes 30
python -m gateway.cli.forecast_cli run --all --every-minutes 30
```

Important CLI flags:

- `--k 10`
- `--history-minutes 180`
- `--horizon-minutes 30`
- `--missing-rate-max 0.1`
- `--storage sqlite|csv`
- `--db-path data/db/telemetry.sqlite`
- `--verbose`

## Tests

Unit tests live under `ml/tests` and cover:

- event detection on synthetic spikes
- filtering reducing spike influence
- feature extraction including time-of-day encoding
- kNN forecast shapes and percentile bands
- MAE and RMSE calculation
