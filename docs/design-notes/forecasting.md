# Forecasting Design Note

## Purpose

This note describes the Layer 4 forecasting pipeline that sits on top of gateway storage and produces a 30-minute continuous forecast for each warehouse pod.

## Pipeline Summary

Pseudocode-level flow:

```text
every 30 minutes:
  for each target pod:
    choose forecast time t0
    read last 3 hours of telemetry
    resample to 1-minute grid
    detect recent event/disturbance
    build filtered baseline window
    extract feature vector
    load case base for this pod
    forecast baseline with analogue kNN or bounded fallback
    if event detected:
      build event-persist slope scenario
    save forecast rows + metadata

  evaluate any saved forecasts whose 30-minute horizon has completed:
    read actual t+1..t+30 values
    compute MAE/RMSE for temperature and RH
    save evaluation rows
    append a new analogue case if data quality is acceptable
```

## Diagram-Friendly Bullet List

- Input source:
  - prefer `data/db/telemetry.sqlite`
  - fall back to `data/raw/pods/<pod_id>/YYYY-MM-DD.csv`
- Rolling forecast window:
  - fixed 3 hours
  - resampled to 1-minute steps
  - 180 points used for features
- Event detector:
  - compute `dT5` and `dRH5`
  - derive robust MAD thresholds
  - trigger on consecutive breaches or hard jumps
  - label as `door_open_like`, `ventilation_issue_like`, or `unknown`
- Baseline preparation:
  - keep raw series unchanged
  - clip event-era deltas to reconstruct a robust baseline
- Similarity features:
  - last temp/RH/dew point
  - 15/30/60 minute slopes
  - 30/60 minute volatility
  - 60 minute min/max
  - cyclical hour encoding with `sin` and `cos`
- Analogue forecast:
  - normalize features from case-base statistics
  - rank nearest historical cases
  - take median future trajectory
  - compute p25 and p75 bands from neighbour spread
- Event-persist scenario:
  - continue current raw 5-minute slope
  - clamp rate to deterministic bounds
- Evaluation and learning:
  - compare forecast vs actual after 30 minutes
  - compute MAE and RMSE for temp and RH
  - log large errors
  - append good-quality windows into the case base

## Operational Notes

- The design avoids classification and heavy model training.
- Dew point is treated as a derived feature, not as an independent sensor requirement.
- Season bins are intentionally deferred.
- The case base grows online, so analogue quality should improve as warehouse history accumulates.
