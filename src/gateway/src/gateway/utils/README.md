# Gateway Utility Helpers

This folder contains small utility modules shared by several gateway
subsystems.

## Files

### `sequence.py`

Contains `sequence_reset_detected`, the helper used to decide whether a pod's
sequence and uptime behaviour indicates a reboot or reset. This logic matters
for correct session tracking in storage and routing.

### `timeutils.py`

Contains:

- `utc_now`
- `utc_now_iso`
- `parse_utc_iso`

These functions keep timestamp handling consistent across ingestion, storage,
forecasting, and diagnostics.

### `backoff.py`

Contains `ExponentialBackoff`, a small helper for retry-oriented workflows.

## Why This Folder Exists

These utilities are intentionally separated so low-level concerns such as time
formatting and sequence-reset detection do not become duplicated differently in
multiple gateway subsystems.
