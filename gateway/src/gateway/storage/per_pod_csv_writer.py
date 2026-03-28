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


@dataclass(frozen=True)
class PerPodWriteResult:
    """Result of writing one normalized multi-source telemetry record."""

    inserted: bool
    duplicate: bool
    path: Path


class PerPodCsvWriter:
    """Write multi-source telemetry into the canonical per-pod raw partitions."""

    def __init__(self, data_root: Path | str | None = None, legacy_log_dir: Path | str | None = None) -> None:
        self.paths: StoragePaths = build_storage_paths(data_root)
        self._writer = RawTelemetryWriter(self.paths.root)
        self._link_writer = LinkQualityWriter(self.paths.root)
        self._legacy_log_dir = Path(legacy_log_dir) if legacy_log_dir is not None else self.paths.root.parent / "gateway" / "logs"
        self._legacy_logger = GatewayCsvLogger(self._legacy_log_dir)

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

    def write_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        self._link_writer.write_snapshot(snapshot)
        self._legacy_logger.log_link_snapshot(snapshot)

    def close(self) -> None:
        self._writer.close()
        self._link_writer.close()
        self._legacy_logger.close()
