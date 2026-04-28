# File overview:
# - Responsibility: Append-only canonical writer for link-quality snapshots.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Append-only canonical writer for link-quality snapshots."""

from __future__ import annotations

from pathlib import Path

from gateway.link.stats import LinkSnapshot
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.raw_writer import CsvAppendWriter
from gateway.storage.schema import LINK_QUALITY_COLUMNS
from gateway.utils.timeutils import parse_utc_iso
# Class purpose: Persist link metrics snapshots into daily CSV partitions.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class LinkQualityWriter:
    """Persist link metrics snapshots into daily CSV partitions."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on LinkQualityWriter.
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
    # Method purpose: Append one snapshot row to the canonical daily
    #   link-quality file.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on LinkQualityWriter.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_snapshot(self, snapshot: LinkSnapshot) -> Path:
        """Append one snapshot row to the canonical daily link-quality file."""
        day = parse_utc_iso(snapshot.ts_pc_utc).date()
        path = self.paths.raw_link_quality_day_path(day)
        self._writer_for(path).write_row(
            {
                "ts_pc_utc": snapshot.ts_pc_utc,
                "pod_id": snapshot.pod_id,
                "connected": 1 if snapshot.connected else 0,
                "last_rssi": snapshot.last_rssi,
                "total_received": snapshot.total_received,
                "total_missing": snapshot.total_missing,
                "total_duplicates": snapshot.total_duplicates,
                "disconnect_count": snapshot.disconnect_count,
                "reconnect_count": snapshot.reconnect_count,
                "missing_rate": f"{snapshot.missing_rate:.6f}",
            }
        )
        return path
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on LinkQualityWriter.
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
    #   method on LinkQualityWriter.
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
            writer = CsvAppendWriter(path, LINK_QUALITY_COLUMNS)
            self._writers[path] = writer
        return writer
