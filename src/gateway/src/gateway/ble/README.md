# BLE Communication Layer

This folder contains the gateway-side Bluetooth Low Energy communication logic
for the physical pod.

## Responsibility

The BLE subsystem is responsible for:

- discovering the physical pod,
- verifying that the expected custom service exists,
- maintaining a session for telemetry notifications and status reads,
- and exposing device-specific behaviour to higher-level ingestion code.

It is deliberately separate from storage and forecasting logic. Its purpose is
to establish and maintain communication, not to interpret warehouse behaviour.

## Files

### `client.py`

Contains:

- `PodTarget`
  - a simple representation of a BLE target device.
- `PodSession`
  - the main session object that manages connection state, notification
    handling, sampling callbacks, and reconnect behaviour.

This is the core BLE runtime file. It matters because the ingester depends on
it to deliver decoded pod samples and communication events.

### `gatt.py`

Contains:

- `GattProfile`
  - a structured description of the expected service and characteristics.
- `profile_from_firmware`
  - derives the gateway-side profile from firmware configuration.
- `ensure_profile_present`
  - verifies that the connected device exposes the expected service layout.
- `iter_service_lines`
  - diagnostic helper for inspecting service information.

This file matters because it formalises the contract between firmware and
gateway.

### `scanner.py`

Contains:

- `ScanMatch`
  - result structure for matching BLE advertisements.
- `_matches_candidate`
  - helper used during device filtering.

This file controls how the gateway decides whether an advertised BLE device
looks like one of the project's pods.

## Design Choices

- The subsystem separates scanning, profile description, and session handling so
  that each concern remains independently testable.
- Profile verification is explicit rather than assumed. This reduces the risk
  of connecting to the wrong device or misreading a partially compatible
  service.

## Dependencies

Called by:

- `gateway/src/gateway/ingesters/ble_ingester.py`

Depends on:

- firmware UUID and flag conventions loaded via the gateway configuration path.

## Limitations

- BLE reliability is inherently influenced by operating system behaviour and
  radio conditions.
- The subsystem is intentionally focused on the project's custom pod profile; it
  is not a general-purpose BLE client library.
