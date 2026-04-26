# Gateway Ingesters

This folder contains the source-specific ingestion adapters that feed live data
into the rest of the gateway.

## Why This Subsystem Exists

The repository uses more than one communication path:

- the real pod speaks Bluetooth Low Energy,
- the synthetic pod cluster speaks newline-delimited JSON over TCP.

The rest of the gateway should not need to understand the details of either
transport. These ingesters exist to convert both sources into the same internal
telemetry record form.

## Files

### `ble_ingester.py`

Contains:

- `BleIngesterSettings`
  - configuration bundle for BLE discovery and session behaviour.
- `BleIngester`
  - launches pod sessions, receives telemetry callbacks, updates link state,
    and pushes normalised records to the shared queue.

Inputs:

- BLE notifications and connection events from `gateway/ble/`.

Outputs:

- `TelemetryRecord` instances placed on the multi-pod queue.

### `tcp_ingester.py`

Contains:

- `TcpIngesterSettings`
  - configuration for the synthetic pod listener.
- `TcpIngester`
  - accepts synthetic pod connections, decodes newline-delimited JSON, records
    corrupt lines, and pushes normalised records to the shared queue.

Inputs:

- TCP connections from the synthetic pod cluster.

Outputs:

- the same `TelemetryRecord` queue items used by the BLE ingester.

### `__init__.py`

Package marker. The real behaviour lives in the two source-specific files.

## Design Notes

- The ingesters are intentionally thin. Their role is source adaptation, not
  heavy analysis.
- Both ingesters register resend or reconnect support with the router so the
  wider gateway can treat each pod consistently.
- Timing or latency instrumentation is added here because this is the point
  where the gateway first accepts a source packet into the live system.

## Dependencies

Called by:

- multi-pod orchestration and gateway command-line runtime.

Depends on:

- `gateway/ble/` for BLE sessions,
- `gateway/protocol/` for decoding,
- `gateway/multi/` for shared record routing.

## Recent Behaviour Changes

- The BLE ingester participates in the improved link-quality tracking used by
  the dashboard health and review pages.
- The TCP ingester supports the expanded synthetic cluster used to demonstrate
  nine synthetic pods plus the physical pod.
