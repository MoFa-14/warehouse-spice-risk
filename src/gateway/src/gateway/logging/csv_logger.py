# File overview:
# - Responsibility: Append-only CSV log writers for samples and link metrics.
# - Project role: Coordinates append-only file writing, locking, and
#   persistence-side buffering.
# - Main data or concerns: CSV rows, write queues, locks, and storage paths.
# - Related flow: Receives normalized records from routing or preprocessing and
#   passes persisted outputs to later analysis.
# - Why this matters: Centralizing write behavior avoids duplicate storage-side
#   assumptions across the gateway.

"""Append-only CSV log writers for samples and link metrics."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Mapping

from gateway.link.stats import LinkSnapshot
from gateway.protocol.decoder import TelemetryRecord
from gateway.protocol.validation import format_quality_flags
from gateway.storage.sample_csv import build_sample_row, ensure_sample_csv_schema


SAMPLE_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "seq",
    "ts_uptime_s",
    "temp_c",
    "rh_pct",
    "dew_point_c",
    "flags",
    "rssi",
    "quality_flags",
]

LINK_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "connected",
    "last_rssi",
    "total_received",
    "total_missing",
    "total_duplicates",
    "disconnect_count",
    "reconnect_count",
    "missing_rate",
]
# Class purpose: Small append-only CSV logger with line buffering.
# - Project role: Belongs to the gateway write and logging pipeline and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Centralizing write behavior avoids duplicate storage-side
#   assumptions across the gateway.
# - Related flow: Receives normalized records from routing or preprocessing and
#   passes persisted outputs to later analysis.

class CsvAppendLogger:
    """Small append-only CSV logger with line buffering."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on CsvAppendLogger.
    # - Inputs: Arguments such as path, fieldnames, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

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
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on CsvAppendLogger.
    # - Inputs: Arguments such as row, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def write_row(self, row: Mapping[str, Any]) -> None:
        serializable = {key: ("" if value is None else value) for key, value in row.items()}
        self._writer.writerow(serializable)
        self._handle.flush()
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on CsvAppendLogger.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Centralizing write behavior avoids duplicate
    #   storage-side assumptions across the gateway.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.flush()
            self._handle.close()
# Class purpose: Own both gateway CSV outputs and provide typed helper methods.
# - Project role: Belongs to the gateway write and logging pipeline and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Centralizing write behavior avoids duplicate storage-side
#   assumptions across the gateway.
# - Related flow: Receives normalized records from routing or preprocessing and
#   passes persisted outputs to later analysis.

class GatewayCsvLogger:
    """Own both gateway CSV outputs and provide typed helper methods."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on GatewayCsvLogger.
    # - Inputs: Arguments such as log_dir, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        ensure_sample_csv_schema(log_dir / "samples.csv", SAMPLE_COLUMNS)
        self.samples = CsvAppendLogger(log_dir / "samples.csv", SAMPLE_COLUMNS)
        self.link_quality = CsvAppendLogger(log_dir / "link_quality.csv", LINK_COLUMNS)
    # Method purpose: Implements the log sample step used by this subsystem.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on GatewayCsvLogger.
    # - Inputs: Arguments such as ts_pc_utc, record, rssi, quality_flags,
    #   interpreted according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Centralizing write behavior avoids duplicate
    #   storage-side assumptions across the gateway.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def log_sample(
        self,
        *,
        ts_pc_utc: str,
        record: TelemetryRecord,
        rssi: int | None,
        quality_flags: tuple[str, ...] | list[str],
    ) -> None:
        self.samples.write_row(
            build_sample_row(
                ts_pc_utc=ts_pc_utc,
                record=record,
                rssi=rssi,
                quality_flags=format_quality_flags(tuple(quality_flags)),
            )
        )
    # Method purpose: Implements the log link snapshot step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on GatewayCsvLogger.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Centralizing write behavior avoids duplicate
    #   storage-side assumptions across the gateway.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def log_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        self.link_quality.write_row(
            {
                "ts_pc_utc": snapshot.ts_pc_utc,
                "pod_id": snapshot.pod_id,
                "connected": str(snapshot.connected).lower(),
                "last_rssi": snapshot.last_rssi,
                "total_received": snapshot.total_received,
                "total_missing": snapshot.total_missing,
                "total_duplicates": snapshot.total_duplicates,
                "disconnect_count": snapshot.disconnect_count,
                "reconnect_count": snapshot.reconnect_count,
                "missing_rate": f"{snapshot.missing_rate:.6f}",
            }
        )
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway write and logging pipeline and acts
    #   as a method on GatewayCsvLogger.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Centralizing write behavior avoids duplicate
    #   storage-side assumptions across the gateway.
    # - Related flow: Receives normalized records from routing or preprocessing
    #   and passes persisted outputs to later analysis.

    def close(self) -> None:
        self.samples.close()
        self.link_quality.close()
