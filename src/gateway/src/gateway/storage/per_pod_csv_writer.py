# File overview:
# - Responsibility: Per-pod canonical CSV writer used by the multi-pod gateway.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Per-pod canonical CSV writer used by the multi-pod gateway."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from gateway.link.stats import LinkSnapshot
from gateway.logging.csv_logger import GatewayCsvLogger
from gateway.multi.record import TelemetryRecord
from gateway.protocol.decoder import TelemetryRecord as ProtocolTelemetryRecord
from gateway.storage.link_writer import LinkQualityWriter
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.raw_writer import RawTelemetryWriter, RawWriteResult
# Class purpose: Result of writing one normalized multi-source telemetry record.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class PerPodWriteResult:
    """Result of writing one normalized multi-source telemetry record."""

    inserted: bool
    duplicate: bool
    path: Path
# Class purpose: Write multi-source telemetry into the canonical per-pod raw
#   partitions.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class PerPodCsvWriter:
    """Write multi-source telemetry into the canonical per-pod raw partitions."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on PerPodCsvWriter.
    # - Inputs: Arguments such as data_root, legacy_log_dir, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def __init__(self, data_root: Path | str | None = None, legacy_log_dir: Path | str | None = None) -> None:
        self.paths: StoragePaths = build_storage_paths(data_root)
        self._writer = RawTelemetryWriter(self.paths.root)
        self._link_writer = LinkQualityWriter(self.paths.root)
        self._legacy_log_dir = Path(legacy_log_dir) if legacy_log_dir is not None else self.paths.root.parent / "gateway" / "logs"
        self._legacy_logger = GatewayCsvLogger(self._legacy_log_dir)
    # Method purpose: Writes record into the configured destination.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on PerPodCsvWriter.
    # - Inputs: Arguments such as record, quality_flags, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: Returns PerPodWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_record(self, record: TelemetryRecord, *, quality_flags: Iterable[str]) -> PerPodWriteResult:
        normalized_quality_flags = tuple(quality_flags)
        protocol_record = ProtocolTelemetryRecord(
            pod_id=record.pod_id,
            seq=record.seq,
            ts_uptime_s=record.ts_uptime_s,
            temp_c=record.temp_c,
            rh_pct=record.rh_pct,
            flags=record.flags,
        )
        result: RawWriteResult = self._writer.write_sample(
            ts_pc_utc=record.ts_pc_utc,
            record=protocol_record,
            rssi=record.rssi,
            quality_flags=normalized_quality_flags,
        )
        if result.inserted:
            self._legacy_logger.log_sample(
                ts_pc_utc=record.ts_pc_utc,
                record=protocol_record,
                rssi=record.rssi,
                quality_flags=normalized_quality_flags,
            )
        return PerPodWriteResult(inserted=result.inserted, duplicate=result.duplicate, path=result.path)
    # Method purpose: Writes link snapshot into the configured destination.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on PerPodCsvWriter.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        self._link_writer.write_snapshot(snapshot)
        self._legacy_logger.log_link_snapshot(snapshot)
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on PerPodCsvWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def close(self) -> None:
        self._writer.close()
        self._link_writer.close()
        self._legacy_logger.close()
