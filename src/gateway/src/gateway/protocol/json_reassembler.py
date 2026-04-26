"""Reassemble JSON objects from fragmented BLE notifications."""

from __future__ import annotations


class JsonReassembler:
    """Buffer UTF-8 text fragments until a full JSON object is available."""

    def __init__(self, max_buffer_size: int = 65536) -> None:
        self.max_buffer_size = max_buffer_size
        self.reset()

    def reset(self) -> None:
        self._buffer: list[str] = []
        self._depth = 0
        self._in_string = False
        self._escape = False

    def feed_bytes(self, payload: bytes | bytearray) -> list[str]:
        return self.feed_text(bytes(payload).decode("utf-8", errors="ignore"))

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
