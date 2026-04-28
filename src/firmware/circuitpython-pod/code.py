# File overview:
# - Responsibility: Phase 1 CircuitPython firmware for the physical sensing pod.
# - Project role: Implements device-side sensing, buffering, status tracking, and
#   BLE behavior on the physical pod.
# - Main data or concerns: Sensor samples, ring-buffer entries, BLE payloads, status
#   fields, and timing values.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.
# - Why this matters: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.

"""Phase 1 CircuitPython firmware for the physical sensing pod.

This file is the top-level runtime loop that executes on the Feather
nRF52840-based pod. In architectural terms, it is the first stage of the
end-to-end monitoring pipeline: it turns a real sensor reading into a compact
telemetry message that the gateway can later decode, validate, store, and
forecast from.

The surrounding modules keep the responsibilities separated:

- ``sensors.py`` owns the SHT45 hardware interaction and error recovery.
- ``ble_service.py`` exposes the custom BLE service used by the gateway.
- ``ring_buffer.py`` keeps recent samples available for replay after reconnects.
- ``status.py`` builds a small status payload so the gateway can inspect pod
  health without parsing telemetry packets.

The main loop here is intentionally simple. The firmware samples on a fixed
interval, publishes the latest reading if a central device is connected, and
accepts a very small control grammar for later phases of the project.
"""

import json
import time

from ble_service import PodBlePeripheral
from config import (
    CONTROL_CMD_REQ_FROM_SEQ,
    CONTROL_CMD_SET_INTERVAL,
    FIRMWARE_VERSION,
    MAX_SAMPLE_INTERVAL_S,
    MIN_SAMPLE_INTERVAL_S,
    POD_ID,
    RING_BUFFER_SIZE,
    SAMPLE_INTERVAL_S,
    TELEMETRY_MAX_LEN,
    device_name,
)
from ring_buffer import RingBuffer
from sensors import SHT45Sensor
from status import PodStatus
# Function purpose: Serialize payloads in a stable compact form.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as payload, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def compact_json(payload):
    """Serialize payloads in a stable compact form.

    The BLE transport budget is small, so this helper removes unnecessary
    whitespace to keep the on-air packet short. Keeping the format stable also
    helps the gateway decoder, tests, and any diagnostic tooling because the
    same logical sample always renders in the same shape.
    """
    return json.dumps(payload).replace(", ", ",").replace(": ", ":")
# Function purpose: Serialize one sample into the pod-to-gateway telemetry contract.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as sample, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def encode_sample_payload(sample):
    """Serialize one sample into the pod-to-gateway telemetry contract.

    This is the exact payload shape that later appears in the gateway decoder
    and then in the ``samples_raw`` storage table. Keeping the firmware-side
    schema explicit is important because everything downstream assumes the same
    keys and units.
    """
    payload = compact_json(
        {
            "pod_id": sample["pod_id"],
            "seq": sample["seq"],
            "ts_uptime_s": sample["ts_uptime_s"],
            "temp_c": sample["temp_c"],
            "rh_pct": sample["rh_pct"],
            "flags": sample["flags"],
        }
    )
    if len(payload) > TELEMETRY_MAX_LEN:
        print("Telemetry payload warning: {} bytes".format(len(payload)))
    return payload
# Function purpose: Normalise control writes into text before parsing.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def normalize_command_value(value):
    """Normalise control writes into text before parsing.

    CircuitPython BLE callbacks can surface values in slightly different forms
    depending on the characteristic interaction. The firmware therefore accepts
    either bytes or text and converts everything to one plain string path before
    command parsing.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)
# Function purpose: Parse the minimal Phase 1 control grammar.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as raw_value, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def parse_control_command(raw_value):
    """Parse the minimal Phase 1 control grammar.

    At this stage of the project the pod only needs to understand a very small
    command surface. The gateway can request resend-related behaviour in later
    phases and can also change the sample interval. The grammar deliberately
    accepts either ``CMD:ARG`` or ``CMD=ARG`` to keep manual testing easy.
    """
    text = normalize_command_value(raw_value).strip()
    if not text:
        return "", ""

    if ":" in text:
        command, argument = text.split(":", 1)
    elif "=" in text:
        command, argument = text.split("=", 1)
    else:
        command, argument = text, ""

    return command.strip().upper(), argument.strip()
# Function purpose: Perform one complete sensing cycle.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as sensor, samples, ble, status_obj, seq, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def take_sample(sensor, samples, ble, status_obj, seq):
    """Perform one complete sensing cycle.

    In project terms this is the moment where the physical environment becomes
    structured telemetry. The function:

    1. asks the sensor wrapper for a fault-tolerant reading,
    2. converts it into the BLE telemetry payload,
    3. stores the sample in the replay buffer for reconnect recovery,
    4. updates the pod status characteristic, and
    5. notifies the connected gateway if a client is currently attached.
    """
    sample = sensor.read_sample(POD_ID, seq, time.monotonic())
    payload = encode_sample_payload(sample)

    samples.append(sample)
    status_obj.set_sensor_error(sensor.last_error_code)
    ble.update_status(status_obj.to_payload())
    if ble.is_connected():
        ble.publish_telemetry(payload)

    print("Sample {}".format(payload))
    return sample
# Function purpose: Boot the pod and keep the acquisition / BLE service loop alive.
# - Project role: Belongs to the embedded firmware runtime layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

def main():
    """Boot the pod and keep the acquisition / BLE service loop alive.

    The startup sequence is intentionally ordered so the pod produces a first
    sample before advertising. That design choice improves the demo experience:
    the gateway can connect and immediately receive a recent observation instead
    of waiting for the next sampling interval boundary.
    """
    pod_name = device_name()
    print("Booting {} firmware {}".format(pod_name, FIRMWARE_VERSION))

    samples = RingBuffer(RING_BUFFER_SIZE)
    sensor = SHT45Sensor()
    status_obj = PodStatus()
    status_obj.set_sensor_error(sensor.last_error_code)

    ble = PodBlePeripheral(pod_name)
    ble.update_status(status_obj.to_payload())

    current_interval_s = SAMPLE_INTERVAL_S
    seq = 1

    # Create a sample before advertising so the first connection can get an
    # immediate replay instead of waiting for the next interval boundary.
    # This makes the system feel responsive during live demos and keeps the
    # gateway from starting with an empty latest-reading state.
    take_sample(sensor, samples, ble, status_obj, seq)
    seq += 1
    next_sample_at = time.monotonic() + current_interval_s

    ble.start_advertising()

    while True:
        now = time.monotonic()

        for event in ble.poll():
            if event["type"] == "connected":
                latest_sample = samples.latest()
                if latest_sample is not None:
                    # Replaying the newest sample avoids an artificial "dead
                    # air" period immediately after connection and gives the
                    # gateway a known-good starting point for sequence tracking.
                    latest_payload = encode_sample_payload(latest_sample)
                    ble.publish_telemetry(latest_payload)
                    print("Re-published latest sample on connect")
                ble.update_status(status_obj.to_payload())
            elif event["type"] == "control":
                command, argument = parse_control_command(event["value"])

                if command == CONTROL_CMD_REQ_FROM_SEQ:
                    print(
                        "REQ_FROM_SEQ stub received for seq {} (Phase 2 resend not implemented)".format(
                            argument or "?"
                        )
                    )
                elif command == CONTROL_CMD_SET_INTERVAL:
                    try:
                        requested_interval = int(argument)
                    except ValueError:
                        print("Invalid SET_INTERVAL payload: {}".format(argument))
                    else:
                        if (
                            requested_interval < MIN_SAMPLE_INTERVAL_S
                            or requested_interval > MAX_SAMPLE_INTERVAL_S
                        ):
                            print(
                                "Rejected SET_INTERVAL {}s outside {}-{}s".format(
                                    requested_interval,
                                    MIN_SAMPLE_INTERVAL_S,
                                    MAX_SAMPLE_INTERVAL_S,
                                )
                            )
                        else:
                            current_interval_s = requested_interval
                            status_obj.set_interval(current_interval_s)
                            ble.update_status(status_obj.to_payload())
                            next_sample_at = now + current_interval_s
                            print(
                                "SET_INTERVAL accepted, new interval={}s".format(
                                    current_interval_s
                                )
                            )
                elif command:
                    print("Unknown control command: {}".format(command))

        if now >= next_sample_at:
            take_sample(sensor, samples, ble, status_obj, seq)
            seq += 1
            next_sample_at = now + current_interval_s

        # A short sleep keeps the main loop responsive without busy-spinning.
        # On microcontroller firmware this matters because a tight spin would
        # waste power and CPU time without improving telemetry quality.
        time.sleep(0.1)


main()
