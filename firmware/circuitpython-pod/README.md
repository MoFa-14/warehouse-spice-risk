# CircuitPython Pod Firmware

Target board: Adafruit Feather nRF52840 Express connected to an SHT45 (SHT4x family) temperature / relative humidity sensor over I2C.

## Source Of Truth

The source-of-truth firmware lives in this folder inside the repo.

- Edit: `firmware/circuitpython-pod/*.py`
- Deploy to board: `firmware/circuitpython-pod/deploy_to_circuitpy.ps1`
- Verify board matches repo: `firmware/circuitpython-pod/verify_deploy.ps1`
- PC-side monitors: `pod_ble_monitor.py` and `pod_serial_monitor.py`

`pod_ble_monitor.py` loads its UUIDs and pod name from this folder's `config.py`, so the desktop test tool follows the repo firmware config instead of its own duplicated constants.

## Hardware

- Board: Adafruit Feather nRF52840 Express
- Sensor: Sensirion SHT45 / Adafruit SHT4x breakout
- I2C wiring assumption: use the board default `SDA` and `SCL` pins (`board.SDA` / `board.SCL`)
- Power: connect sensor `VIN` to `3V` and sensor `GND` to `GND`
- I2C pull-ups: most SHT45 breakout boards already include them; do not add extra pull-ups unless your hardware requires it

## Install CircuitPython

1. Double-tap the board `RESET` button to enter the UF2 bootloader.
2. A USB mass-storage drive appears, typically named `BOOT`.
3. Download the CircuitPython UF2 build for the Adafruit Feather nRF52840 Express from [circuitpython.org](https://circuitpython.org/board/feather_nrf52840_express/).
4. Drag and drop the UF2 file onto the `BOOT` drive.
5. After reboot, a new drive named `CIRCUITPY` should appear.

Reference used: [CircuitPython downloads / board page](https://circuitpython.org/board/feather_nrf52840_express/) and Adafruit's CircuitPython workflow docs that describe the `BOOT` to `CIRCUITPY` UF2 flow.

## Deploy The Pod Code

Copy these repository files to the root of the `CIRCUITPY` drive:

- `firmware/circuitpython-pod/code.py`
- `firmware/circuitpython-pod/config.py`
- `firmware/circuitpython-pod/ble_service.py`
- `firmware/circuitpython-pod/sensors.py`
- `firmware/circuitpython-pod/ring_buffer.py`
- `firmware/circuitpython-pod/status.py`

Or use:

```powershell
.\firmware\circuitpython-pod\deploy_to_circuitpy.ps1 -DriveLetter E
```

Then verify the deployed files match the repo:

```powershell
.\firmware\circuitpython-pod\verify_deploy.ps1 -DriveLetter E
```

Copy the required libraries into `CIRCUITPY/lib` from the matching Adafruit CircuitPython bundle:

- `adafruit_ble/`
- `adafruit_sht4x.py`
- `adafruit_bus_device/`

Notes:

- `board`, `busio`, `json`, and `time` are built into CircuitPython and do not need to be copied.
- Keep only the libraries above on-device to save flash.
- The repo's `firmware/circuitpython-pod/lib/` folder is intentionally just a placeholder and should not store a full bundle in git.

After copying:

1. Safely eject `CIRCUITPY`.
2. Press `RESET` once or let auto-reload restart the script.
3. Open the serial console at 115200 baud.
4. Confirm the boot log, sensor init result, advertising log, and sample output.

## Runtime Behavior

- Advertises as `SHT45-POD-XX` where `XX` comes from `POD_ID` in `config.py`
- Samples the SHT45 once every 5 seconds for the current development/testing phase
- Maintains a monotonic sequence counter starting at `1` on each boot
- Stores the last `N` samples in an in-memory ring buffer (`RING_BUFFER_SIZE`, default `120`)
- Keeps sampling while disconnected
- On BLE connection, re-notifies the most recent telemetry payload once, then continues five-second telemetry notifications
- The target production/default cadence can be moved back to 60 seconds later by updating `SAMPLE_INTERVAL_S`

## BLE Test With nRF Connect Or BLE Explorer

1. Power the board and open the serial console.
2. In the BLE app, scan for `SHT45-POD-01` (or your configured pod id).
3. Connect and verify the custom service UUID:
   - `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0001`
4. Verify the characteristics:
   - Telemetry (`NOTIFY`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0002`
   - Control (`WRITE`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0003`
   - Status (`READ`): `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0004`
5. Read the Status characteristic and confirm the returned compact value `firmware_version,last_sensor_error_code,current_interval_s`.
6. Subscribe to Telemetry notifications immediately after connecting.
7. Confirm you receive one latest-sample notification right after subscribe/connect and then a new notification every 5 seconds.

Example telemetry payload:

```json
{"pod_id":"01","seq":12,"ts_uptime_s":660.1,"temp_c":21.34,"rh_pct":54.12,"flags":0}
```

Example status payload:

```text
0.1.0-phase1,0,5
```

## Gateway Integration Note

`ble_read.py` will need to subscribe to the Telemetry characteristic (Notify) and parse JSON payload.

The current gateway-side sample in [ble_read.py](/C:/Users/TERA%20MAX/Desktop/DSP/src/ble_read.py) still uses a Nordic UART UUID. For this Phase 1 pod firmware, extend it later like this:

- Scan/connect to the pod as usual
- Call `start_notify()` on the Telemetry characteristic UUID immediately after connecting
- Decode the UTF-8 JSON payload and deduplicate later by `(pod_id, seq)`
- Optionally read the Status characteristic for firmware/error/interval metadata

UUIDs to use later in `ble_read.py`:

- Service UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0001`
- Telemetry UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0002`
- Control UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0003`
- Status UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0004`

See also:

- [protocol.md](/C:/Users/TERA%20MAX/Desktop/DSP/src/firmware/circuitpython-pod/protocol.md)
- [docs/design-notes/pod-phase1.md](/C:/Users/TERA%20MAX/Desktop/DSP/src/docs/design-notes/pod-phase1.md)

Primary source references used while shaping this firmware:

- [Adafruit BLE library docs](https://docs.circuitpython.org/projects/ble/en/stable/characteristics.html)
- [Adafruit SHT4x library docs](https://docs.circuitpython.org/projects/sht4x/)
