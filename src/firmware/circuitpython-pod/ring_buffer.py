# File overview:
# - Responsibility: Small fixed-size ring buffer for recent telemetry samples.
# - Project role: Implements device-side sensing, buffering, status tracking, and
#   BLE behavior on the physical pod.
# - Main data or concerns: Sensor samples, ring-buffer entries, BLE payloads, status
#   fields, and timing values.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.
# - Why this matters: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.

"""Small fixed-size ring buffer for recent telemetry samples."""
# Class purpose: Encapsulates the RingBuffer responsibilities used by this module.
# - Project role: Belongs to the embedded firmware runtime layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Gateway decoding and storage rely on the firmware keeping
#   telemetry semantics consistent.
# - Related flow: Reads sensors and pod state, then exposes telemetry and control
#   behavior to the gateway.

class RingBuffer:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: Arguments such as capacity, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def __init__(self, capacity):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = capacity
        self._items = [None] * capacity
        self._count = 0
        self._next_index = 0
    # Method purpose: Implements the capacity step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    @property
    def capacity(self):
        return self._capacity
    # Method purpose: Implements the len step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def __len__(self):
        return self._count
    # Method purpose: Appends append to the accumulated state or storage target.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: Arguments such as item, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def append(self, item):
        self._items[self._next_index] = item
        self._next_index = (self._next_index + 1) % self._capacity
        if self._count < self._capacity:
            self._count += 1
    # Method purpose: Retrieves the latest latest available to the caller.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def latest(self):
        if self._count == 0:
            return None
        return self._items[(self._next_index - 1) % self._capacity]
    # Method purpose: Implements the to list step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def to_list(self):
        # Return samples oldest-to-newest, which is the most useful order for
        # future resend logic and for manual inspection during debugging.
        ordered = []
        start = (self._next_index - self._count) % self._capacity
        for offset in range(self._count):
            ordered.append(self._items[(start + offset) % self._capacity])
        return ordered
    # Method purpose: Implements the iter from seq step used by this subsystem.
    # - Project role: Belongs to the embedded firmware runtime layer and acts as
    #   a method on RingBuffer.
    # - Inputs: Arguments such as start_seq, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Gateway decoding and storage rely on the firmware
    #   keeping telemetry semantics consistent.
    # - Related flow: Reads sensors and pod state, then exposes telemetry and
    #   control behavior to the gateway.

    def iter_from_seq(self, start_seq):
        for item in self.to_list():
            if item["seq"] >= start_seq:
                yield item
