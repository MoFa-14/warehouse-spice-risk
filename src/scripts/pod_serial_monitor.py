# File overview:
# - Responsibility: Implements the pod serial monitor logic for this project area.
# - Project role: Provides convenience entry points for monitoring, forecasting, and
#   evaluation workflows.
# - Main data or concerns: Command-line options, runtime handles, and script-level
#   control flow.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.
# - Why this matters: Scripts matter because they are the shortest operational path
#   into the project for routine runs.

import argparse
import sys
import time

import serial
import serial.tools.list_ports
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
    parser = argparse.ArgumentParser(description="Watch CircuitPython USB serial logs from the pod.")
    parser.add_argument("--port", default="COM6", help="Serial port, for example COM6.")
    parser.add_argument("--baudrate", type=int, default=115200, help="Serial baudrate.")
    parser.add_argument("--duration", type=int, default=0, help="Seconds to watch. 0 means forever.")
    parser.add_argument("--list", action="store_true", help="List available serial ports and exit.")
    return parser.parse_args()
# Function purpose: Lists ports for later iteration or selection.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def list_ports():
    print("Available ports:")
    for port in serial.tools.list_ports.comports():
        print(f" - {port.device}: {port.description}")
# Function purpose: Dispatches the top-level script entry point and forwards
#   command-line arguments into the underlying runtime path.
# - Project role: Belongs to the operator automation script layer and contributes
#   one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Scripts matter because they are the shortest operational
#   path into the project for routine runs.
# - Related flow: Wraps lower runtime modules into directly executable operational
#   scripts.

def main():
    args = parse_args()
    if args.list:
        list_ports()
        return 0

    end_time = time.time() + args.duration if args.duration > 0 else None
    print(f"Opening {args.port} at {args.baudrate} baud...")
    with serial.Serial(args.port, args.baudrate, timeout=0.2) as ser:
        while True:
            # CircuitPython sends plain UTF-8 console output over the USB CDC
            # serial port, so we can stream and print it directly.
            chunk = ser.read(4096)
            if chunk:
                sys.stdout.write(chunk.decode("utf-8", errors="replace"))
                sys.stdout.flush()
            if end_time and time.time() >= end_time:
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
