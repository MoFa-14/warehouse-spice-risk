# Firmware Subsystem

This folder contains the software that runs on the physical sensing pod. The
firmware is the first executable stage in the overall system because it is
responsible for converting real environmental conditions into digital telemetry.

## Why This Subsystem Exists

The rest of the repository depends on the pod producing dependable
temperature-and-relative-humidity readings in a predictable communication
format. Without the firmware layer:

- the gateway would have no live hardware data source,
- the dashboard would only be able to show synthetic telemetry,
- and the forecasting system would not be demonstrably connected to a real
  physical environment.

The firmware is intentionally lightweight. It does not try to perform
forecasting, long-term storage, or complex analytics. Its role is to sense,
package, and expose telemetry safely.

## Folder Structure

This folder currently contains one deployable pod implementation:

- `circuitpython-pod/`
  - CircuitPython runtime files for the Feather nRF52840 plus SHT45 pod.

## Design Notes

The firmware design makes several deliberate trade-offs:

- simplicity over feature breadth,
- recoverable sensor error handling instead of crash-on-failure behaviour,
- compact telemetry formatting for BLE transport,
- and minimal control commands that are easy to inspect and test.

These choices keep the pod understandable and reliable enough for a prototype
that must integrate with a gateway, storage layer, and dashboard.

## Important Relationships

The firmware interacts most directly with:

- `gateway/src/gateway/ble/`
  - BLE discovery, profile handling, and session management on the gateway.
- `gateway/src/gateway/protocol/decoder.py`
  - decoding of the payloads produced by the pod.
- `gateway/src/gateway/protocol/validation.py`
  - validation of decoded telemetry values and firmware-set flags.

The pod-side UUIDs, command names, payload keys, and status encoding therefore
form a contract that must remain consistent with the gateway package.

## Limitations and Cautions

- The current firmware is intentionally small and does not implement the full
  resend flow suggested by some control stubs.
- The pod is designed for prototype monitoring, not hardened field deployment.
- A single physical pod cannot represent the full variability of a real
  warehouse, which is why the repository also includes the synthetic pod
  cluster.

For detailed file-by-file documentation, see:

- `firmware/circuitpython-pod/README.md`
