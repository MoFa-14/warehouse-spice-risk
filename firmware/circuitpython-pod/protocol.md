# Pod BLE Protocol (Phase 1)

## Fixed UUIDs

- Service UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0001`
- Telemetry characteristic UUID (`NOTIFY`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0002`
- Control characteristic UUID (`WRITE`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0003`
- Status characteristic UUID (`READ`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0004`

These UUIDs are fixed constants and must stay stable so the gateway script can discover and subscribe reliably.

## Telemetry Payload Format

Phase 1 uses UTF-8 encoded JSON for each sample notification:

```json
{"pod_id":"01","seq":123,"ts_uptime_s":4567.1,"temp_c":21.34,"rh_pct":54.12,"flags":0}
```

Field meanings:

- `pod_id`: string pod identifier, always present
- `seq`: monotonic sample sequence counter, starts at `1` on boot
- `ts_uptime_s`: uptime seconds from `time.monotonic()` because wall-clock time is not guaranteed in Phase 1
- `temp_c`: temperature in degrees Celsius, or `null` if sensor read failed
- `rh_pct`: relative humidity percent, or `null` if sensor read failed
- `flags`: bitfield for sample state

Gateway dedupe key for later phases: `(pod_id, seq)`.

## Status Payload Format

The read-only Status characteristic returns a compact UTF-8 string in this fixed order:

```text
firmware_version,last_sensor_error_code,current_interval_s
```

Current development/testing example:

```text
0.1.0-phase1,0,60
```

Sensor error code meanings:

- `0`: no sensor error
- `1`: sensor init failed
- `2`: sensor read failed

## Flags Bit Definitions

| Bit | Mask | Meaning |
| --- | --- | --- |
| 0 | `0x01` | `SENSOR_ERROR` - the sample contains a sensor init/read failure |
| 1 | `0x02` | `LOW_BATT` placeholder for a future low-battery signal |
| 2-7 | n/a | Reserved for future use |

## Control Characteristic (Phase 1 Stubs)

The Control characteristic accepts simple UTF-8 text commands. Phase 1 only includes minimal placeholders:

- `REQ_FROM_SEQ:<n>` or `REQ_FROM_SEQ=<n>`
  - Parsed and logged only
  - Full resend / catch-up logic is deferred to Phase 2
- `SET_INTERVAL:<seconds>` or `SET_INTERVAL=<seconds>`
  - Updates the in-memory sample interval for the current boot session
  - Allowed range: `5` to `3600` seconds
- The gateway now requests `SET_INTERVAL:60` by default so the pod emits readings every sixty seconds and the gateway stores them at that cadence

Unknown or malformed commands are ignored after logging to the serial console.

## Gateway Note

The existing gateway sample [ble_read.py](/C:/Users/TERA%20MAX/Desktop/DSP/src/ble_read.py) should later:

1. Connect to the pod.
2. Subscribe to Telemetry notifications immediately after connect.
3. Decode the UTF-8 JSON payload.
4. Optionally read Status for firmware/error/interval metadata.
