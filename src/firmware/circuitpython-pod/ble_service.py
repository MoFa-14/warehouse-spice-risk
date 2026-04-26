"""BLE service definition for the physical pod.

This module is the communication boundary between the firmware and the gateway.
It wraps the lower-level CircuitPython BLE primitives so the main firmware loop
can reason in project terms such as "connected", "control write received", and
"publish telemetry sample" instead of handling GATT details directly.
"""

from adafruit_ble import BLERadio
from adafruit_ble.advertising.standard import ProvideServicesAdvertisement
from adafruit_ble.characteristics import Characteristic
from adafruit_ble.characteristics.stream import StreamOut
from adafruit_ble.characteristics.string import StringCharacteristic
from adafruit_ble.services import Service
from adafruit_ble.uuid import VendorUUID

from config import (
    CONTROL_CHAR_UUID,
    SERVICE_UUID,
    STATUS_CHAR_UUID,
    TELEMETRY_CHAR_UUID,
    TELEMETRY_MAX_LEN,
)


class PodTelemetryService(Service):
    """Custom service that mirrors the pod communication contract.

    The service exposes three characteristics:

    - ``telemetry`` for outbound notifications carrying one sensor sample,
    - ``control`` for small text commands written by the gateway, and
    - ``status`` for a compact read-only health summary.

    Keeping all three together under one vendor UUID makes the pod easy to
    discover and keeps the on-pod protocol explicit.
    """

    uuid = VendorUUID(SERVICE_UUID)

    telemetry = StreamOut(
        uuid=VendorUUID(TELEMETRY_CHAR_UUID),
        buffer_size=TELEMETRY_MAX_LEN,
        properties=Characteristic.NOTIFY,
    )

    control = StringCharacteristic(
        uuid=VendorUUID(CONTROL_CHAR_UUID),
        properties=Characteristic.WRITE,
        initial_value=b"",
    )

    status = StringCharacteristic(
        uuid=VendorUUID(STATUS_CHAR_UUID),
        properties=Characteristic.READ,
        initial_value=b"",
    )


class PodBlePeripheral:
    """Present BLERadio as a small event-driven peripheral wrapper.

    The firmware loop polls this object and receives simplified events rather
    than dealing with the full BLE state machine. That keeps the main runtime
    readable and makes it easier to explain the communication behaviour as a
    sequence of connection, control, status, and telemetry steps.
    """

    def __init__(self, device_name):
        self.device_name = device_name
        self.radio = BLERadio()
        self.service = PodTelemetryService()
        self.advertisement = ProvideServicesAdvertisement(self.service)
        self._was_connected = False
        self._advertising_started = False

        try:
            self.radio.name = device_name
        except Exception as exc:
            print("BLE radio name warning: {}".format(exc))

        # Let the library manage the scan response name automatically. That
        # proved more discoverable on Windows than forcing complete_name here.
        self.service.control = ""
        self.service.status = ""

    def start_advertising(self):
        if not self.radio.connected and not self._advertising_started:
            self.radio.start_advertising(self.advertisement)
            self._advertising_started = True
            print("BLE advertising as {}".format(self.device_name))

    def publish_telemetry(self, payload):
        try:
            self.service.telemetry.write(payload.encode("utf-8"))
        except Exception as exc:
            print("Telemetry notify warning: {}".format(exc))

    def update_status(self, payload):
        self.service.status = payload

    def is_connected(self):
        return self.radio.connected

    def poll(self):
        """Translate BLE state into simple firmware-loop events.

        The output of this method is consumed directly in ``code.py``. It is
        effectively the bridge between lower-level radio state and the pod's
        higher-level application logic.
        """
        events = []
        connected = self.radio.connected

        if connected and not self._was_connected:
            self._advertising_started = False
            print("BLE connected")
            events.append({"type": "connected"})
        elif self._was_connected and not connected:
            self._advertising_started = False
            print("BLE disconnected")
            events.append({"type": "disconnected"})

        self._was_connected = connected

        control_value = self.service.control
        if control_value:
            print("BLE control write: {}".format(control_value))
            events.append({"type": "control", "value": control_value})
            self.service.control = ""

        if not connected and not self._advertising_started:
            self.start_advertising()

        return events
