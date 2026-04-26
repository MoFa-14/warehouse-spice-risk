"""Helpers for newline-delimited JSON framing."""

from __future__ import annotations

import json
from typing import Any


def encode_ndjson_line(payload: dict[str, Any]) -> bytes:
    """Serialize one JSON payload followed by a newline."""
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")


def decode_ndjson_line(payload: bytes | str) -> dict[str, Any]:
    """Parse one newline-delimited JSON payload."""
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)
    return json.loads(text.strip())
