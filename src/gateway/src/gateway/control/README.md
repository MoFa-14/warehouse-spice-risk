# Gateway Control Helpers

This folder contains small control-path abstractions used by the gateway when
it needs to request retransmission or related behaviour from an ingestion
source.

## Files

### `resend.py`

Contains:

- `ResendController`
  - protocol describing a resend-capable controller.
- `BleResendController`
  - BLE-specific resend/control wrapper.
- `TcpResendController`
  - TCP-specific resend/control wrapper for synthetic pods.

## Why It Matters

The router and ingesters should not care about the transport-specific details
of issuing a resend request. This folder provides that abstraction so the
multi-pod router can respond to gaps or corrupt records in a unified way.
