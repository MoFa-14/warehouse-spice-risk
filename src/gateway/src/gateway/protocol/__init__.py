"""Protocol decoding and validation helpers."""
# File overview:
# - Responsibility: Protocol decoding and validation helpers.
# - Project role: Decodes transport payloads and enforces schema-level telemetry
#   rules.
# - Main data or concerns: JSON fragments, NDJSON lines, decoded telemetry fields,
#   and validation results.
# - Related flow: Receives raw text or bytes and passes validated structured
#   payloads to ingestion.


from gateway.protocol.decoder import DecodeError, StatusRecord, TelemetryRecord, decode_status_payload, decode_telemetry_payload
from gateway.protocol.ndjson import decode_ndjson_line, encode_ndjson_line

__all__ = [
    "DecodeError",
    "StatusRecord",
    "TelemetryRecord",
    "decode_ndjson_line",
    "decode_status_payload",
    "decode_telemetry_payload",
    "encode_ndjson_line",
]
