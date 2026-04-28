# File overview:
# - Responsibility: Implements the pod BLE monitor logic for this project area.
# - Project role: Provides convenience entry points for monitoring, forecasting, and
#   evaluation workflows.
# - Main data or concerns: Command-line options, runtime handles, and script-level
#   control flow.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.
# - Why this matters: Scripts matter because they are the shortest operational path
#   into the project for routine runs.

import argparse
import asyncio
import importlib.util
import json
from pathlib import Path

from bleak import BleakClient, BleakScanner

SRC_ROOT = Path(__file__).resolve().parents[1]
FIRMWARE_DIR = SRC_ROOT / "firmware" / "circuitpython-pod"
CONFIG_PATH = FIRMWARE_DIR / "config.py"
POD_ADDRESS = "F2:9A:41:2B:5B:55"
# Function purpose: Loads firmware config into the structure expected by downstream
#   code.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def load_firmware_config():
    spec = importlib.util.spec_from_file_location("pod_firmware_config", CONFIG_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load firmware config from {CONFIG_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FIRMWARE_CONFIG = load_firmware_config()
POD_NAME = FIRMWARE_CONFIG.device_name()
SERVICE_UUID = FIRMWARE_CONFIG.SERVICE_UUID.lower()
TELEMETRY_UUID = FIRMWARE_CONFIG.TELEMETRY_CHAR_UUID.lower()
CONTROL_UUID = FIRMWARE_CONFIG.CONTROL_CHAR_UUID.lower()
STATUS_UUID = FIRMWARE_CONFIG.STATUS_CHAR_UUID.lower()
# Class purpose: Encapsulates the JsonChunkAssembler responsibilities used by this
#   module.
# - Project role: Belongs to the operator automation script layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

class JsonChunkAssembler:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the operator automation script layer and acts
    #   as a method on JsonChunkAssembler.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Wraps lower runtime modules into directly executable
    #   operational scripts.

    def __init__(self):
        self.buffer = ""
        self.depth = 0
        self.in_string = False
        self.escape = False
    # Method purpose: Implements the feed step used by this subsystem.
    # - Project role: Belongs to the operator automation script layer and acts
    #   as a method on JsonChunkAssembler.
    # - Inputs: Arguments such as text, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Scripts matter because they are the shortest
    #   operational path into the project for routine runs.
    # - Related flow: Wraps lower runtime modules into directly executable
    #   operational scripts.

    def feed(self, text: str):
        complete = []
        for char in text:
            if self.depth == 0 and char in "\r\n\t ":
                continue

            self.buffer += char

            if self.in_string:
                if self.escape:
                    self.escape = False
                elif char == "\\":
                    self.escape = True
                elif char == '"':
                    self.in_string = False
                continue

            if char == '"':
                self.in_string = True
            elif char == "{":
                self.depth += 1
            elif char == "}" and self.depth > 0:
                self.depth -= 1
                if self.depth == 0:
                    complete.append(self.buffer)
                    self.buffer = ""
        return complete
# Function purpose: Parses args into structured values.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def parse_args():
    parser = argparse.ArgumentParser(description="Monitor the SHT45 pod over BLE.")
    parser.add_argument("--name", default=POD_NAME, help="BLE advertising name to look for.")
    parser.add_argument("--address", default=POD_ADDRESS, help="BLE address to use first.")
    parser.add_argument("--duration", type=int, default=30, help="How long to listen for notifications.")
    parser.add_argument("--scan-timeout", type=float, default=15.0, help="Scan timeout in seconds.")
    parser.add_argument("--list-only", action="store_true", help="Only scan and print discovered devices.")
    parser.add_argument("--dump-services", action="store_true", help="Connect and print the discovered GATT services/chars.")
    parser.add_argument("--uncached", action="store_true", help="Request uncached GATT discovery on Windows.")
    return parser.parse_args()


JSON_ASSEMBLER = JsonChunkAssembler()
# Function purpose: Implements the on notify step used by this subsystem.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as _, data, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def on_notify(_, data: bytearray):
    text = data.decode("utf-8", errors="ignore")
    for message in JSON_ASSEMBLER.feed(text):
        print(f"notify: {message}")
        try:
            print("json:", json.loads(message))
        except json.JSONDecodeError:
            pass
# Function purpose: Lists devices for later iteration or selection.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as scan_timeout, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

async def list_devices(scan_timeout: float):
    print(f"Using firmware config: {CONFIG_PATH}")
    print(f"Scanning for {scan_timeout:.1f}s...")
    devices = await BleakScanner.discover(timeout=scan_timeout, return_adv=True)
    for device, adv in devices.values():
        print(
            "seen:",
            adv.local_name or device.name,
            device.address,
            adv.service_uuids or [],
        )
    return devices
# Function purpose: Finds target for later processing.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as name, address, scan_timeout, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

async def find_target(name: str, address: str, scan_timeout: float):
    if address:
        print(f"Looking up device by address {address}...")
        dev = await BleakScanner.find_device_by_address(address, timeout=scan_timeout)
        if dev:
            return dev

    devices = await list_devices(scan_timeout)
    for device, adv in devices.values():
        local_name = adv.local_name or device.name
        uuids = [uuid.lower() for uuid in (adv.service_uuids or [])]
        if local_name == name or SERVICE_UUID in uuids:
            return device
    return None
# Function purpose: Dumps services for diagnostics or inspection.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as client, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def dump_services(client):
    print("Discovered services:")
    for service in client.services:
        print(" SERVICE", service.uuid, service.description)
        for char in service.characteristics:
            print("   CHAR", char.uuid, list(char.properties))
# Function purpose: Implements the monitor step used by this subsystem.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as args, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

async def monitor(args):
    if args.list_only:
        await list_devices(args.scan_timeout)
        return 0

    print(f"Using firmware config: {CONFIG_PATH}")
    print(f"Expected pod name: {args.name}")
    print(f"Expected telemetry UUID: {TELEMETRY_UUID}")

    device = await find_target(args.name, args.address, args.scan_timeout)
    if not device:
        print("Pod not found. Make sure the board is advertising and not already connected elsewhere.")
        return 1

    print(f"Connecting to {device.name or args.name} ({device.address})")
    winrt_options = {"use_cached_services": False} if args.uncached else {}
    client = BleakClient(device, winrt=winrt_options)
    try:
        await client.connect(timeout=args.scan_timeout)
        print("Connected:", client.is_connected)
        if not client.is_connected:
            print("Connection did not complete on this Windows BLE stack.")
            print("Try again after turning Bluetooth off/on, closing other BLE apps, or rebooting the adapter.")
            return 2

        dump_services(client)
        if args.dump_services:
            return 0

        try:
            status = await client.read_gatt_char(STATUS_UUID)
            print("status:", status.decode("utf-8", errors="ignore"))
        except Exception as exc:
            print("status read warning:", exc)

        print("telemetry uuid:", TELEMETRY_UUID)
        print("control uuid:", CONTROL_UUID)

        await client.start_notify(TELEMETRY_UUID, on_notify)
        print(f"Listening for {args.duration}s... Press Ctrl+C to stop early.")
        await asyncio.sleep(args.duration)
        await client.stop_notify(TELEMETRY_UUID)
        print("Done.")
        return 0
    except Exception as exc:
        print("BLE monitor error:", exc)
        return 3
    finally:
        if client.is_connected:
            await client.disconnect()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(monitor(parse_args())))
    except KeyboardInterrupt:
        print("Stopped by user.")
