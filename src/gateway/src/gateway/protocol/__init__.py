"""Protocol decoding and validation helpers."""

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
