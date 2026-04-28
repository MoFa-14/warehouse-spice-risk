# File overview:
# - Responsibility: Append-only raw telemetry writer with file-backed dedupe.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Append-only raw telemetry writer with file-backed dedupe."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.sample_csv import build_sample_row, ensure_sample_csv_schema
from gateway.storage.schema import RAW_SAMPLE_COLUMNS, QualityFlag, has_quality_flag, parse_quality_mask, quality_flags_to_mask
from gateway.utils.timeutils import parse_utc_iso
# Class purpose: Small line-buffered CSV append helper shared across Layer 3
#   writers.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class CsvAppendWriter:
    """Small line-buffered CSV append helper shared across Layer 3 writers."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on CsvAppendWriter.
    # - Inputs: Arguments such as path, fieldnames, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def __init__(self, path: Path, fieldnames: list[str]) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists() and self.path.stat().st_size > 0
        self._handle = self.path.open("a", encoding="utf-8", newline="", buffering=1)
        self._writer = csv.DictWriter(self._handle, fieldnames=fieldnames)
        if not file_exists:
            self._writer.writeheader()
            self._handle.flush()
    # Method purpose: Writes row into the configured destination.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on CsvAppendWriter.
    # - Inputs: Arguments such as row, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_row(self, row: Mapping[str, Any]) -> None:
        serializable = {key: ("" if value is None else value) for key, value in row.items()}
        self._writer.writerow(serializable)
        self._handle.flush()
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on CsvAppendWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.flush()
            self._handle.close()
# Class purpose: Outcome of attempting to append a raw sample row.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class RawWriteResult:
    """Outcome of attempting to append a raw sample row."""

    inserted: bool
    duplicate: bool
    path: Path
# Class purpose: Persist canonical raw telemetry rows into per-pod, per-day CSV
#   files.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class RawTelemetryWriter:
    """Persist canonical raw telemetry rows into per-pod, per-day CSV files."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on RawTelemetryWriter.
    # - Inputs: Arguments such as data_root, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def __init__(self, data_root: Path | str | None = None) -> None:
        self.paths: StoragePaths = build_storage_paths(data_root)
        self.paths.ensure_base_dirs()
        self._writers: dict[Path, CsvAppendWriter] = {}
        self._seen_by_path: dict[Path, set[int]] = {}
    # Method purpose: Append one raw sample row unless the sequence is already
    #   stored.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on RawTelemetryWriter.
    # - Inputs: Arguments such as ts_pc_utc, record, rssi, quality_flags,
    #   interpreted according to the rules encoded in the body below.
    # - Outputs: Returns RawWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_sample(
        self,
        *,
        ts_pc_utc: str,
        record: TelemetryRecord,
        rssi: int | None,
        quality_flags: Iterable[str],
    ) -> RawWriteResult:
        """Append one raw sample row unless the sequence is already stored."""
        day = parse_utc_iso(ts_pc_utc).date()
        path = self.paths.raw_pod_day_path(record.pod_id, day)
        ensure_sample_csv_schema(path, RAW_SAMPLE_COLUMNS)
        seen_sequences = self._load_seen_sequences(path)
        quality_mask = quality_flags_to_mask(quality_flags)

        if has_quality_flag(quality_mask, QualityFlag.SEQUENCE_RESET):
            seen_sequences.clear()

        if record.seq in seen_sequences:
            return RawWriteResult(inserted=False, duplicate=True, path=path)

        self._writer_for(path).write_row(
            build_sample_row(
                ts_pc_utc=ts_pc_utc,
                record=record,
                rssi=rssi,
                quality_flags=quality_mask,
            )
        )
        seen_sequences.add(record.seq)
        return RawWriteResult(inserted=True, duplicate=False, path=path)
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on RawTelemetryWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()
    # Method purpose: Implements the writer for step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on RawTelemetryWriter.
    # - Inputs: Arguments such as path, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns CsvAppendWriter when the function completes
    #   successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _writer_for(self, path: Path) -> CsvAppendWriter:
        writer = self._writers.get(path)
        if writer is None:
            ensure_sample_csv_schema(path, RAW_SAMPLE_COLUMNS)
            writer = CsvAppendWriter(path, RAW_SAMPLE_COLUMNS)
            self._writers[path] = writer
        return writer
    # Method purpose: Loads seen sequences into the structure expected by
    #   downstream code.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on RawTelemetryWriter.
    # - Inputs: Arguments such as path, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns set[int] when the function completes successfully.
    # - Important decisions: The transformation rules here define how later code
    #   interprets the same data, so the shape of the output needs to stay
    #   stable and reproducible.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _load_seen_sequences(self, path: Path) -> set[int]:
        seen = self._seen_by_path.get(path)
        if seen is not None:
            return seen

        ensure_sample_csv_schema(path, RAW_SAMPLE_COLUMNS)
        seen = set()
        if path.exists() and path.stat().st_size > 0:
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    quality_mask = parse_quality_mask(row.get("quality_flags"))
                    if has_quality_flag(quality_mask, QualityFlag.SEQUENCE_RESET):
                        seen.clear()
                    seq_text = str(row.get("seq", "")).strip()
                    if seq_text:
                        seen.add(int(seq_text))
        self._seen_by_path[path] = seen
        return seen
