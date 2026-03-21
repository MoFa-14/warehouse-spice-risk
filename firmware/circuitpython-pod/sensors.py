"""SHT45 sensor wrapper with retry-friendly error handling."""

import adafruit_sht4x
import board
import busio

from config import (
    FLAG_SENSOR_ERROR,
    SENSOR_ERROR_INIT_FAILED,
    SENSOR_ERROR_NONE,
    SENSOR_ERROR_READ_FAILED,
)


class SHT45Sensor:
    """Own the I2C bus and sensor object so read failures can recover cleanly."""

    def __init__(self):
        self._i2c = None
        self._sensor = None
        self.serial_number = None
        self.last_error_code = SENSOR_ERROR_NONE
        self._init_sensor(initial_boot=True)

    def _init_sensor(self, initial_boot=False):
        try:
            if self._i2c is None:
                self._i2c = busio.I2C(board.SCL, board.SDA)

            self._sensor = adafruit_sht4x.SHT4x(self._i2c)
            self._sensor.mode = adafruit_sht4x.Mode.NOHEAT_HIGHPRECISION
            self.serial_number = self._sensor.serial_number
            self.last_error_code = SENSOR_ERROR_NONE
            print(
                "Sensor init success: SHT45 serial=0x{:08X}".format(
                    self.serial_number
                )
            )
            return True
        except Exception as exc:
            self._sensor = None
            self.serial_number = None
            self.last_error_code = SENSOR_ERROR_INIT_FAILED
            prefix = "Sensor init failure" if initial_boot else "Sensor re-init failure"
            print("{}: {}".format(prefix, exc))
            return False

    def read_sample(self, pod_id, seq, ts_uptime_s):
        """Return a payload-shaped sample even when the sensor fails to read."""
        flags = 0
        temp_c = None
        rh_pct = None

        # If a previous read failed, try to rebuild the driver before giving up.
        if self._sensor is None and not self._init_sensor():
            flags |= FLAG_SENSOR_ERROR
            return {
                "pod_id": pod_id,
                "seq": seq,
                "ts_uptime_s": round(ts_uptime_s, 1),
                "temp_c": temp_c,
                "rh_pct": rh_pct,
                "flags": flags,
            }

        try:
            temp_c, rh_pct = self._sensor.measurements
            temp_c = round(temp_c, 2)
            rh_pct = round(rh_pct, 2)
            self.last_error_code = SENSOR_ERROR_NONE
        except Exception as exc:
            self.last_error_code = SENSOR_ERROR_READ_FAILED
            self._sensor = None
            flags |= FLAG_SENSOR_ERROR
            print("Sensor read failure: {}".format(exc))

        return {
            "pod_id": pod_id,
            "seq": seq,
            "ts_uptime_s": round(ts_uptime_s, 1),
            "temp_c": temp_c,
            "rh_pct": rh_pct,
            "flags": flags,
        }
