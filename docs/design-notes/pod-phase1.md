# Pod Phase 1 Design Notes

## Scope

This phase implements only the pod firmware on the Adafruit Feather nRF52840 Express with an SHT45 sensor.

Explicitly not implemented here:

- gateway-to-pod reliability logic beyond a control stub
- gateway-to-server data flow
- dashboard / Flask UI work

## Final Phase 1 Behavior

- The pod samples temperature and relative humidity from the SHT45 sensor.
- Each sample carries `pod_id`, `seq`, `ts_uptime_s`, `temp_c`, `rh_pct`, and `flags`.
- The pod advertises as a BLE peripheral and exposes a custom service with telemetry, control, and status characteristics.
- The pod keeps sampling while disconnected.
- The last `120` samples are retained in a RAM ring buffer.
- On connect, the latest sample is re-notified immediately, then periodic notifications continue.

## Decisions

- Payload format: compact UTF-8 JSON for readability and easy extension of the existing gateway-side `ble_read.py`
- Timestamp source: `time.monotonic()` uptime seconds (`ts_uptime_s`) because no RTC or network time is assumed
- BLE role: pod is a peripheral that advertises a fixed custom service
- Reliability baseline: keep sampling while disconnected and retain the last `120` samples in RAM
- Connect behavior: notify the latest telemetry sample once on connect, then continue once-per-minute notifications
- Status encoding: short CSV-like text so it fits safely in a simple read characteristic without extra framing
- Desktop test workflow: desktop scripts read constants from the repo firmware config so the PC monitor follows the same source of truth as the board code

## Fixed BLE UUIDs

- Service: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0001`
- Telemetry: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0002`
- Control: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0003`
- Status: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0004`

## Testing Lessons From The Demo Setup

- The board and repo can drift if `CIRCUITPY` is not updated after a config change.
- The repository now includes deploy and verification scripts to make that drift visible immediately.
- Windows can discover the correct advertisement name and service UUID while still caching an older GATT table from a previous firmware image.
- The desktop BLE monitor now has an `--uncached` mode and a JSON assembler so the demo output is cleaner and easier to trust.

## Recommended Demo Flow

1. Verify the board files match the repo with `verify_deploy.ps1`.
2. Watch USB serial logs with `pod_serial_monitor.py`.
3. Scan the pod over BLE with `pod_ble_monitor.py --list-only`.
4. Connect and observe notifications with `pod_ble_monitor.py --duration 70 --uncached`.

## Gateway Follow-Up

`ble_read.py` should later switch from the Nordic UART sample UUID to the Telemetry UUID above, subscribe immediately after connecting, then parse the JSON notifications and deduplicate by `(pod_id, seq)`.
