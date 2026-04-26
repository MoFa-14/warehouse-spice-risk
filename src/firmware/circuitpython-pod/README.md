# CircuitPython Physical Pod

This folder contains the deployable firmware for the physical telemetry pod.
The pod is the hardware endpoint that measures real temperature and relative
humidity and exposes the readings to the gateway over Bluetooth Low Energy.

## Subsystem Responsibility

The code in this folder is responsible for:

- sensor acquisition from the SHT45,
- management of the Bluetooth Low Energy service,
- buffering of recent samples for reconnect replay,
- publication of status and telemetry payloads,
- interpretation of a small control-command surface,
- and deployment support for copying firmware files onto the board.

## Why It Matters in the Overall Project

This folder contains the code that makes the prototype physically grounded. It
is the source of Pod `01`, the one real pod in the system. The gateway,
forecasting layer, and dashboard all rely on this firmware to provide data in
the expected schema and timing structure.

## File Inventory

### `code.py`

This is the top-level firmware entry point. It:

- boots the pod,
- creates the sensor, ring buffer, and BLE service objects,
- takes the initial reading before advertising,
- enters the periodic sampling loop,
- republishes the latest sample on connection,
- and processes control commands such as interval changes.

Important functions include:

- `compact_json`
  - formats telemetry JSON compactly for BLE transmission.
- `encode_sample_payload`
  - builds the exact wire payload sent to the gateway.
- `parse_control_command`
  - interprets the minimal text command grammar.
- `take_sample`
  - performs one sensing cycle and optional BLE publication.
- `main`
  - coordinates the whole firmware runtime.

### `ble_service.py`

This file defines the custom BLE service used by the gateway.

Important classes:

- `PodTelemetryService`
  - declares the custom service and its telemetry, control, and status
    characteristics.
- `PodBlePeripheral`
  - wraps the BLE radio in a small event-oriented interface used by `code.py`.

This file matters because it is the pod-side implementation of the BLE contract
that the gateway later discovers and consumes.

### `config.py`

This file contains firmware configuration constants such as:

- pod identifier,
- firmware version,
- sample interval,
- telemetry payload limits,
- service and characteristic UUIDs,
- command names,
- and flag or error-code constants.

The configuration file matters because both the pod behaviour and the gateway's
expectations depend on these values staying consistent.

### `ring_buffer.py`

This file contains `RingBuffer`, a small fixed-size in-memory sample buffer used
to keep recent readings available. The primary reason it exists is to support
immediate replay of the newest sample when the gateway connects or reconnects.

### `sensors.py`

This file contains `SHT45Sensor`, which owns the I2C bus and sensor driver and
returns payload-shaped samples even when a read fails.

This design matters because the rest of the firmware should not need to know
how to recover from low-level sensor faults.

### `status.py`

This file contains `PodStatus`, which builds the compact status payload
published over BLE. The status characteristic allows the gateway to inspect
firmware version, interval, and recent sensor error state separately from the
main telemetry characteristic.

### `deploy_to_circuitpy.ps1`

Deployment helper that copies the firmware files to the CircuitPython device
storage. This script is part of the engineering workflow rather than the live
runtime.

### `verify_deploy.ps1`

Deployment verification helper used to confirm that the expected files are
present after deployment.

## Design Choices

Several design choices are visible in this folder:

- The firmware publishes compact JSON rather than a binary custom protocol in
  order to keep the wire format easy to inspect and easy to decode in Python.
- The firmware collects an immediate first sample before advertising so the
  gateway can receive a meaningful latest value as soon as it connects.
- The control surface is intentionally tiny. The project focuses on monitoring
  and prediction, not on complex pod-side actuation logic.

## Limitations

- The resend command path is still largely a stub and not a complete
  retransmission subsystem.
- The firmware is designed for prototype use, not for hardened production
  operation under all environmental conditions.
- The pod performs sensing and communication only. All forecasting, evaluation,
  and storage happen elsewhere in the repository.
