# Warehouse Spice Risk

## Phase 1 Status

Phase 1 in this workspace is the pod firmware for an Adafruit Feather nRF52840 Express with an SHT45 temperature / humidity sensor.

This repo copy is organized so the firmware source in `firmware/circuitpython-pod/` is the single source of truth, and the board's `CIRCUITPY` drive is treated as a deployment target.

## What Is Included

- CircuitPython pod firmware in `firmware/circuitpython-pod/`
- BLE protocol notes in `firmware/circuitpython-pod/protocol.md`
- Phase 1 design notes in `docs/design-notes/pod-phase1.md`
- Desktop BLE monitor in `pod_ble_monitor.py`
- Desktop USB serial log monitor in `pod_serial_monitor.py`
- Deploy / verify helpers for the board in `firmware/circuitpython-pod/deploy_to_circuitpy.ps1` and `firmware/circuitpython-pod/verify_deploy.ps1`

## Source Of Truth Workflow

1. Edit the firmware files under `firmware/circuitpython-pod/`.
2. Deploy those exact files to `CIRCUITPY`.
3. Verify the board copy matches the repo copy.
4. Test over USB serial and BLE using the desktop helper scripts.

This avoids a common demo problem where the desktop test script reflects new settings but the board is still running an older on-device file.

## Quick Start

Create and use the local test environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install bleak pyserial
```

Deploy to the board:

```powershell
powershell -ExecutionPolicy Bypass -File .\firmware\circuitpython-pod\deploy_to_circuitpy.ps1 -DriveLetter E
powershell -ExecutionPolicy Bypass -File .\firmware\circuitpython-pod\verify_deploy.ps1 -DriveLetter E
```

Watch USB serial logs:

```powershell
.\.venv\Scripts\python.exe .\pod_serial_monitor.py --port COM6
```

Scan for the pod over BLE:

```powershell
.\.venv\Scripts\python.exe .\pod_ble_monitor.py --list-only --scan-timeout 8
```

Listen for telemetry notifications:

```powershell
.\.venv\Scripts\python.exe .\pod_ble_monitor.py --duration 70 --uncached
```

## Current BLE Contract

- Device name: `SHT45-POD-01`
- Service UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0001`
- Telemetry UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0002`
- Control UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0003`
- Status UUID: `7f12b100-7c2d-4b6a-8f4b-8a1f0e3c0004`

Telemetry payload example:

```json
{"pod_id":"01","seq":123,"ts_uptime_s":4567.1,"temp_c":21.34,"rh_pct":54.12,"flags":0}
```

## Windows Notes

- PowerShell may block `Activate.ps1`; using `.\.venv\Scripts\python.exe ...` works without changing the execution policy.
- Windows can cache older BLE GATT layouts for the same device address. If a service or characteristic appears stale, try `--uncached`, close other BLE apps, or remove the device from Windows Bluetooth settings and reconnect.
- `verify_deploy.ps1` is the quickest way to confirm whether the board is actually running the repo version of each firmware file.
