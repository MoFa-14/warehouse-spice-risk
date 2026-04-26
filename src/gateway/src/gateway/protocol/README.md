# Gateway Protocol Layer

This folder contains the code that translates raw wire payloads into typed
Python records and quality annotations.

## Responsibility

The protocol layer answers three questions:

1. Can this payload be decoded?
2. If it can be decoded, does it contain the fields the project expects?
3. If it decodes successfully, are any values suspicious or incomplete?

This layer matters because forecasting and dashboard analysis should only happen
after the gateway has established a reliable interpretation of the incoming
message.

## Files

### `decoder.py`

Contains:

- `DecodeError`
  - raised when a payload cannot be parsed safely.
- `StatusRecord`
  - typed representation of the pod status characteristic.
- `TelemetryRecord`
  - typed representation of a telemetry sample.
- `decode_status_payload`
  - parses the compact status message.
- `decode_telemetry_payload`
  - parses telemetry JSON from bytes, text, or an already-parsed mapping.

This file is the formal contract boundary between raw transport content and
structured gateway data.

### `validation.py`

Contains:

- `ValidationResult`
  - quality-flag result object.
- `validate_telemetry`
  - checks for missing values, out-of-range values, and firmware error bits.
- `format_quality_flags`
  - converts quality flags into the compact storage representation.

This file matters because the gateway preserves bad or incomplete measurement
attempts as flagged rows instead of erasing them.

### `ndjson.py`

Contains:

- `encode_ndjson_line`
  - formats a JSON payload as one newline-terminated line.
- `decode_ndjson_line`
  - parses one newline-delimited JSON line.

This file is used by the synthetic TCP path.

### `json_reassembler.py`

Contains `JsonReassembler`, which helps reconstruct complete JSON objects from
fragmented input when a transport delivers partial chunks.

## Design Choices

- Decoding and validation are separate because a payload can be syntactically
  valid yet still contain suspicious environmental values.
- Quality problems are preserved as metadata rather than causing silent data
  loss.

## Limitations

- Validation thresholds are intentionally simple engineering checks, not a full
  physical-model consistency engine.
- The layer validates the shape and basic plausibility of messages, not whether
  they constitute a good forecasting case.
