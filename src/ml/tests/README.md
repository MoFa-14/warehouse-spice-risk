# Forecasting Tests

This folder contains automated tests for the forecasting package.

## Purpose

The forecasting subsystem contains many compact but important pieces of logic:

- feature extraction,
- event detection,
- analogue matching,
- scenario generation,
- and error evaluation.

These tests exist to catch regressions in those areas before they appear as
misleading dashboard plots or incorrect stored forecast rows.

## Main Test Files

### `test_features.py`

Validates feature extraction behaviour from regularised history windows.

### `test_event_detection.py`

Checks that event detection behaves sensibly under disturbance-like and
non-disturbance-like sequences.

### `test_filtering.py`

Validates event-aware baseline window construction.

### `test_knn_forecast.py`

Covers analogue forecast behaviour, including:

- re-anchoring,
- relative-humidity regime gating,
- recency preference,
- and fallback or blending under weak analogue support.

### `test_scenario.py`

Validates event-persist scenario generation, including damped relative-humidity
continuation behaviour.

### `test_evaluator.py`

Checks post-hoc error calculation.

### `_helpers.py` and `fixtures/`

Support utilities and fixture data used across tests.

## What These Tests Prove

They prove that the implemented forecasting logic behaves consistently with the
current design. They do not prove that the model is already optimal for all
warehouse conditions.
