# File overview:
# - Responsibility: Helpers for newline-delimited JSON framing.
# - Project role: Decodes transport payloads and enforces schema-level telemetry
#   rules.
# - Main data or concerns: JSON fragments, NDJSON lines, decoded telemetry fields,
#   and validation results.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.
# - Why this matters: Protocol rules are foundational because storage and
#   forecasting assume the payload contract is already normalized.

"""Helpers for newline-delimited JSON framing."""

from __future__ import annotations

import json
from typing import Any
# Function purpose: Serialize one JSON payload followed by a newline.
# - Project role: Belongs to the gateway protocol parsing and validation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as payload, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns bytes when the function completes successfully.
# - Important decisions: Protocol rules are foundational because storage and
#   forecasting assume the payload contract is already normalized.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.

def encode_ndjson_line(payload: dict[str, Any]) -> bytes:
    """Serialize one JSON payload followed by a newline."""
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
# Function purpose: Parse one newline-delimited JSON payload.
# - Project role: Belongs to the gateway protocol parsing and validation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as payload, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns dict[str, Any] when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.

def decode_ndjson_line(payload: bytes | str) -> dict[str, Any]:
    """Parse one newline-delimited JSON payload."""
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)
    return json.loads(text.strip())
