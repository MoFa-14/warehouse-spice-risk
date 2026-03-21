"""Phase 1 CircuitPython firmware for the SHT45 BLE sensor pod."""

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


def compact_json(payload):
    """Keep notifications short and stable for the future gateway parser."""
    return json.dumps(payload).replace(", ", ",").replace(": ", ":")


def encode_sample_payload(sample):
    """Serialize one sensor sample using the Phase 1 wire format."""
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


def normalize_command_value(value):
    """Accept either raw BLE bytes or already-decoded strings."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def parse_control_command(raw_value):
    """Phase 1 accepts a tiny text command grammar for later expansion."""
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


def take_sample(sensor, samples, ble, status_obj, seq):
    """Read the sensor, cache the sample, and publish it if a client is connected."""
    sample = sensor.read_sample(POD_ID, seq, time.monotonic())
    payload = encode_sample_payload(sample)

    samples.append(sample)
    status_obj.set_sensor_error(sensor.last_error_code)
    ble.update_status(status_obj.to_payload())
    if ble.is_connected():
        ble.publish_telemetry(payload)

    print("Sample {}".format(payload))
    return sample


def main():
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
        time.sleep(0.1)


main()
