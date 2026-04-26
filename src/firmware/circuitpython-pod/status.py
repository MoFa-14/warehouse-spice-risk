"""Helpers for the read-only BLE status payload."""

from config import FIRMWARE_VERSION, SAMPLE_INTERVAL_S, SENSOR_ERROR_NONE


class PodStatus:
    """Track the tiny status snapshot exposed over the BLE read characteristic."""

    def __init__(self):
        self.firmware_version = FIRMWARE_VERSION
        self.interval_s = SAMPLE_INTERVAL_S
        self.last_sensor_error_code = SENSOR_ERROR_NONE

    def set_interval(self, interval_s):
        self.interval_s = interval_s

    def set_sensor_error(self, error_code):
        self.last_sensor_error_code = error_code

    def to_payload(self):
        # Keep the status string compact so it fits a plain StringCharacteristic.
        return "{},{},{}".format(
            self.firmware_version,
            self.last_sensor_error_code,
            self.interval_s,
        )
