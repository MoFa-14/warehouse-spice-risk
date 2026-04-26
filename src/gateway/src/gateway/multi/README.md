# Multi-Pod Routing Layer

This folder contains the internal queue record type and routing logic used when
the gateway is receiving telemetry from multiple pods concurrently.

## Why This Subsystem Exists

The project combines one physical pod and many synthetic pods. The rest of the
gateway should not need to maintain a different pipeline for each source. This
folder exists to provide a shared language and routing layer for multi-pod
operation.

## Files

### `record.py`

Contains `TelemetryRecord`, the shared internal representation used after source
ingestion. This record is the common format seen by validation, storage, and
forecasting orchestration.

### `router.py`

Contains:

- `PodStats`
- `PodRouter`

The router tracks per-pod communication state, missing counts, reconnects,
received signal strength indication updates, and resend-controller associations.
It is important because diagnostics and link-quality snapshots depend on the
router's state.

### `orchestrator.py`

Contains:

- `MultiGatewaySettings`
- `MultiGatewayOrchestrator`

This file coordinates the running combination of ingesters, router, and storage
pipeline for the live mixed-source gateway mode.

## Design Notes

- The internal shared record format is what makes physical and synthetic paths
  converge into one coherent system.
- Routing and statistics are kept separate from persistence so communication
  health can be tracked before data is written.

## Limitations

- This subsystem is focused on transport unification and diagnostics, not on
  business logic such as threshold interpretation or forecasting.
