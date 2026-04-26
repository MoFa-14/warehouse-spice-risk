# Dashboard Package

This folder contains the Flask dashboard application and its tests.

## Purpose

The dashboard is the main interpretation layer for the project. It is not
responsible for sensing or forecasting itself. Instead it reads stored system
outputs and presents them in a form that allows a user to inspect:

- current pod conditions,
- threshold context,
- historical telemetry,
- forecast scenarios,
- persistence comparison,
- and historical forecast behaviour.

## Structure

- `app/`
  - Flask application code, templates, data-access helpers, and services.
- `tests/`
  - dashboard-focused automated tests.
- `requirements.txt`
  - dashboard runtime dependencies.

## Design Philosophy

The dashboard follows a layered structure:

- routes in `app/main.py` stay thin,
- data-access modules read stored files or SQLite,
- service modules prepare view-ready context objects,
- templates render that context.

This separation matters because the dashboard should explain stored system
behaviour, not recompute gateway logic directly.

## Important Behaviour

The dashboard now includes:

- dynamically discovered pods,
- gap-aware history plots,
- a persistence-comparison chart based on completed forecast windows,
- and the dedicated `Pod 1 Forecasting Test` historical analysis card.

For subsystem details, see:

- `dashboard/app/README.md`
- `dashboard/tests/README.md`
