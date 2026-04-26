# Forecasting Core

This folder contains the repository's forecasting logic. It defines the model's
domain objects, feature engineering, event detection, baseline analogue
forecasting, event-persist scenario generation, and post-hoc evaluation.

## Why This Subsystem Exists

The gateway needs a forecasting engine that can be reasoned about in project
terms rather than hidden inside an opaque external library. This package exists
to encode the forecasting design explicitly:

- what the history window looks like,
- how it is summarised,
- how recent events are detected,
- how analogue cases are matched,
- how baseline and event-persist trajectories are generated,
- and how completed forecasts are evaluated.

## Files

### `models.py`

Defines the main data structures:

- `TimeSeriesPoint`
- `EventDetectionResult`
- `FeatureVector`
- `ForecastTrajectory`
- `ForecastBundle`
- `CaseRecord`
- `EvaluationMetrics`

These types make the forecasting pipeline explicit and are used throughout the
gateway orchestration code.

### `config.py`

Contains `ForecastConfig` and `build_config`. This file gathers the main
forecasting assumptions and thresholds in one place so the model design remains
inspectable.

### `features.py`

Contains `extract_feature_vector` and related helpers. It turns a regular
history window into the summary used for analogue matching.

### `event_detection.py`

Contains `detect_recent_event` and the supporting threshold logic used to judge
whether the latest behaviour looks like a disturbance.

### `filtering.py`

Contains `build_baseline_window`, which constructs an event-robust history view
for baseline forecasting when the recent tail contains a disturbance.

### `case_base.py`

Contains `CaseBaseStore` plus case-reading helpers. This file is responsible for
the historical example library used by the analogue forecaster.

### `knn_forecaster.py`

Contains `AnalogueKNNForecaster` and the detailed similarity, regime-gating,
recency, fallback, and neighbour-aggregation logic behind the baseline forecast.

### `scenario.py`

Contains `build_event_persist_forecast`, the alternate scenario used when recent
behaviour is classified as event-like.

### `dewpoint.py`

Contains the dew-point derivation function used to keep forecasted temperature,
relative humidity, and dew point internally consistent.

### `evaluator.py`

Contains `evaluate_forecast`, which compares predicted trajectories against
realised outcomes and produces error metrics used later by storage and the
dashboard.

### `scheduler.py`

Contains `ForecastScheduler`, which defines timing-oriented behaviour for when
forecasts should be considered due.

### `utils.py`

Contains shared statistical and time helpers used across the forecasting
subsystem.

## Recent Behaviour Changes

This folder contains the model-side parts of recent corrections:

- baseline improvement comparison now uses persistence through stored
  evaluation metrics rather than first-ever error comparisons,
- analogue futures are re-anchored to the current live reading,
- relative-humidity matching now includes stronger humidity/dew regime checks
  and a recency preference,
- relative-humidity event-persist behaviour is damped rather than extending the
  raw short-term slope without bound.

## Limitations

- The analogue model is still constrained by the size and representativeness of
  the historical case base.
- Seasonal awareness remains relatively limited.
- Dew point is derived rather than modelled directly, so dew-point behaviour
  inherits any upstream temperature or relative-humidity forecast errors.
