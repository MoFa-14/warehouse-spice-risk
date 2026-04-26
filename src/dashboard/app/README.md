# Dashboard Application

This folder contains the Flask application code for the monitoring and
forecasting dashboard.

## Responsibilities

The application layer is responsible for:

- configuring Flask,
- defining routes,
- loading stored telemetry and forecast outputs,
- constructing page-ready view models,
- and rendering templates.

It does **not** run the forecasting model itself. Forecasts are generated
earlier by the gateway and then read here as stored artefacts.

## Folder Structure

- `main.py`
  - Flask app creation and route registration.
- `data_access/`
  - low-level readers for SQLite and CSV history.
- `services/`
  - page-oriented business logic and chart construction.
- `web/`
  - templates and static assets used for rendering.
- `runtime/`
  - runtime files created by the dashboard such as acknowledgement state.

## Important File

### `main.py`

This file creates the Flask app, registers filters, creates shared runtime
paths, and defines route handlers for:

- overview,
- pod detail,
- alerts,
- health,
- prediction,
- review,
- and a small latest-reading API endpoint.

The route functions stay intentionally thin so the service layer remains the
main place where dashboard logic is explained.
