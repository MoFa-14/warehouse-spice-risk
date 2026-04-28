# File overview:
# - Responsibility: Reassemble JSON objects from fragmented BLE notifications.
# - Project role: Decodes transport payloads and enforces schema-level telemetry
#   rules.
# - Main data or concerns: JSON fragments, NDJSON lines, decoded telemetry fields,
#   and validation results.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.
# - Why this matters: Protocol rules are foundational because storage and
#   forecasting assume the payload contract is already normalized.

"""Reassemble JSON objects from fragmented BLE notifications."""

from __future__ import annotations
# Class purpose: Buffer UTF-8 text fragments until a full JSON object is available.
# - Project role: Belongs to the gateway protocol parsing and validation layer and
#   groups related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Protocol rules are foundational because storage and
#   forecasting assume the payload contract is already normalized.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.

class JsonReassembler:
    """Buffer UTF-8 text fragments until a full JSON object is available."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway protocol parsing and validation
    #   layer and acts as a method on JsonReassembler.
    # - Inputs: Arguments such as max_buffer_size, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives raw text or bytes and passes validated structured
    #   payloads to ingestion.

    def __init__(self, max_buffer_size: int = 65536) -> None:
        self.max_buffer_size = max_buffer_size
        self.reset()
    # Method purpose: Implements the reset step used by this subsystem.
    # - Project role: Belongs to the gateway protocol parsing and validation
    #   layer and acts as a method on JsonReassembler.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Protocol rules are foundational because storage and
    #   forecasting assume the payload contract is already normalized.
    # - Related flow: Receives raw text or bytes and passes validated structured
    #   payloads to ingestion.

    def reset(self) -> None:
        self._buffer: list[str] = []
        self._depth = 0
        self._in_string = False
        self._escape = False
    # Method purpose: Implements the feed bytes step used by this subsystem.
    # - Project role: Belongs to the gateway protocol parsing and validation
    #   layer and acts as a method on JsonReassembler.
    # - Inputs: Arguments such as payload, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns list[str] when the function completes successfully.
    # - Important decisions: Protocol rules are foundational because storage and
    #   forecasting assume the payload contract is already normalized.
    # - Related flow: Receives raw text or bytes and passes validated structured
    #   payloads to ingestion.

    def feed_bytes(self, payload: bytes | bytearray) -> list[str]:
        return self.feed_text(bytes(payload).decode("utf-8", errors="ignore"))
    # Method purpose: Implements the feed text step used by this subsystem.
    # - Project role: Belongs to the gateway protocol parsing and validation
    #   layer and acts as a method on JsonReassembler.
    # - Inputs: Arguments such as text, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns list[str] when the function completes successfully.
    # - Important decisions: Protocol rules are foundational because storage and
    #   forecasting assume the payload contract is already normalized.
    # - Related flow: Receives raw text or bytes and passes validated structured
    #   payloads to ingestion.

    def feed_text(self, text: str) -> list[str]:
        complete: list[str] = []

        for character in text:
            if self._depth == 0 and not self._buffer:
                if character.isspace():
                    continue
                if character != "{":
                    continue

            self._buffer.append(character)

            if self._in_string:
                if self._escape:
                    self._escape = False
                elif character == "\\":
                    self._escape = True
                elif character == '"':
                    self._in_string = False
                continue

            if character == '"':
                self._in_string = True
            elif character == "{":
                self._depth += 1
            elif character == "}":
                if self._depth > 0:
                    self._depth -= 1
                if self._depth == 0:
                    complete.append("".join(self._buffer))
                    self.reset()

            if len(self._buffer) > self.max_buffer_size:
                self.reset()

        return complete
