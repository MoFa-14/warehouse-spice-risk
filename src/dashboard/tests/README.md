# Dashboard Tests

This folder contains automated tests for the Flask dashboard and its service
layer.

## Purpose

The dashboard is an interpretation layer. A failure here can mislead the user
even if the gateway stored correct data. These tests therefore protect:

- data loading,
- latest-reading selection,
- chart building,
- threshold interpretation,
- forecast presentation,
- and route rendering.

## Main Test Areas

### Core Page and Service Behaviour

- `test_routes_smoke.py`
- `test_data_path.py`
- `test_review_service.py`
- `test_alerts.py`

These tests check that the Flask pages and context-building services remain
usable.

### Forecasting Presentation

- `test_prediction_service.py`
- `test_forecast_test_service.py`

These are important recent additions. They validate:

- persistence-based comparison data,
- historical forecast-test-card session selection,
- and forecast-versus-actual reconstruction behaviour.

### Time-Series and Threshold Behaviour

- `test_timeseries_service.py`
- `test_thresholds.py`
- `test_timezone_display.py`
- `test_telemetry_adjustments.py`

These tests protect user-facing chart and interpretation logic.

## Testing Scope

The dashboard tests verify that stored project outputs are interpreted
consistently. They do not generate the forecasts themselves; that behaviour is
validated in the gateway and forecasting test suites.
