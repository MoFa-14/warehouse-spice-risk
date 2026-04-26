# Dashboard Web Templates and Static Assets

This folder contains the presentation layer of the Flask dashboard.

## Purpose

The service layer prepares context objects and Plotly HTML fragments. The files
in this folder decide how that information appears in the browser.

## Templates

### `base.html`

Shared page shell, global layout, and common assets.

### `index.html`

Overview page showing the dashboard landing view.

### `pod_detail.html`

Per-pod page showing latest state, threshold interpretation, plot-range
selection, historical charts, and the most recent forecast panel for that pod.

### `prediction.html`

Main forecasting page. It assembles the prediction panels and the historical
forecast test card.

### `_prediction_panel.html`

Reusable forecast panel partial that renders forecast plots, persistence
comparison, and summary cards.

### `_forecast_test_panel.html`

Partial for the `Pod 1 Forecasting Test` historical analysis section.

### `review.html`, `health.html`, `alerts.html`

Templates for review, health, and alert-oriented views.

## Static Assets

The CSS under `static/css/` contains the styling work that supports the current
dashboard look, including:

- card-based summaries,
- improved threshold presentation,
- plot container layout,
- and the historical forecast-analysis section.

## Important Note

The logic in these files is mostly presentational. Forecasting behaviour,
evaluation logic, and historical selection logic live in the service layer and
should be read there first when trying to understand system behaviour.
