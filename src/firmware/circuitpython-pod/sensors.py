"""SHT45 sensor wrapper with retry-friendly error handling.

The rest of the firmware should not need to know about I2C setup, sensor driver
quirks, or how to recover from intermittent hardware failures. This module
encapsulates that responsibility so the main loop can always ask for a
payload-shaped sample, even when the underlying sensor read fails.
"""

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
    """Own the sensor lifecycle and expose fault-tolerant sampling.

    The gateway and dashboard need consistent telemetry records, not exceptions.
    This class therefore handles both initialisation and recovery. When a read
    fails it preserves the packet structure, sets the error flag, and lets the
    rest of the system decide how to treat the missing measurement.
    """

    def __init__(self):
        self._i2c = None
        self._sensor = None
        self.serial_number = None
        self.last_error_code = SENSOR_ERROR_NONE
        self._init_sensor(initial_boot=True)

    def _init_sensor(self, initial_boot=False):
        """Create or recreate the CircuitPython sensor driver instance.

        Re-initialisation is used both on first boot and after read failures.
        Treating recovery as a first-class path matters in a warehouse monitor,
        because temporary bus or sensor faults should not crash the whole pod.
        """
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
        """Return one telemetry-ready reading record.

        The return type mirrors the BLE telemetry payload on purpose. That keeps
        the rest of the firmware simple and avoids a second translation layer
        between sensor acquisition and radio publication.
        """
        flags = 0
        temp_c = None
        rh_pct = None

        # If a previous read failed, try to rebuild the driver before giving up.
        # This keeps transient hardware issues from becoming permanent until a
        # manual reboot, which is important for unattended sensing.
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
