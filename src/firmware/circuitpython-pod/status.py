# File overview:
# - Responsibility: Helpers for the read-only BLE status payload.
# - Project role: Implements device-side sensing, buffering, status tracking, and
#   BLE behavior on the physical pod.
# - Main data or concerns: Sensor samples, ring-buffer entries, BLE payloads, status
#   fields, and timing values.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.
# - Why this matters: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.

"""Helpers for the read-only BLE status payload."""

from config import FIRMWARE_VERSION, SAMPLE_INTERVAL_S, SENSOR_ERROR_NONE
# Class purpose: Track the tiny status snapshot exposed over the BLE read
#   characteristic.
# - Project role: Belongs to the embedded firmware runtime layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

class PodStatus:
    """Track the tiny status snapshot exposed over the BLE read characteristic."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on PodStatus.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def __init__(self):
        self.firmware_version = FIRMWARE_VERSION
        self.interval_s = SAMPLE_INTERVAL_S
        self.last_sensor_error_code = SENSOR_ERROR_NONE
    # Method purpose: Implements the set interval step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on PodStatus.
    # - Inputs: Arguments such as interval_s, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def set_interval(self, interval_s):
        self.interval_s = interval_s
    # Method purpose: Implements the set sensor error step used by this
    #   subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on PodStatus.
    # - Inputs: Arguments such as error_code, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def set_sensor_error(self, error_code):
        self.last_sensor_error_code = error_code
    # Method purpose: Implements the to payload step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on PodStatus.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def to_payload(self):
        # Keep the status string compact so it fits a plain StringCharacteristic.
        return "{},{},{}".format(
            self.firmware_version,
            self.last_sensor_error_code,
            self.interval_s,
        )
