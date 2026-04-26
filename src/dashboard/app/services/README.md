# Dashboard Services

This folder contains the dashboard's main business-logic layer.

## Purpose

Service modules convert stored project data into dashboard-ready context
objects, charts, and summaries. They are where the dashboard interprets the
system, but not where raw database queries or HTML presentation are defined.

## Main Files

### `pod_service.py`

Builds the dashboard's latest-reading view of each pod. It decides which recent
raw or processed reading should represent the current pod state.

### `timeseries_service.py`

Builds the temperature, relative-humidity, and dew-point history charts for pod
pages. It resolves time windows, handles gaps, and prepares Plotly figures.

### `prediction_service.py`

The main forecast presentation service. It reads stored forecasts and
evaluations, builds baseline and event-persist scenario views, prepares the
persistence-comparison chart, and constructs summary cards.

### `forecast_test_service.py`

Builds the `Pod 1 Forecasting Test` section. It selects the best historical
Pod `01` session, reconstructs completed attempts, and prepares the historical
forecast-versus-actual visualisations.

### `review_service.py`

Builds longer-window review context that combines telemetry and link-quality
history for monitoring analysis.

### `alerts_service.py`

Derives dashboard alert entries and acknowledgement behaviour from current pod
readings and threshold classifications.

### `link_service.py`

Builds health and link-quality context used by the dashboard's health-related
pages.

### `thresholds.py`

Defines storage-condition interpretation thresholds and trajectory-level
classification used throughout the dashboard.

### `telemetry_adjustments.py`

Loads optional dashboard calibration and smoothing settings and applies them to
telemetry before presentation.

## Recent Behaviour Changes

This folder contains many of the user-facing forecasting corrections, including:

- persistence-based comparison logic,
- richer forecast summaries,
- historical forecast test views,
- and display support for the restored event-persist slope.
