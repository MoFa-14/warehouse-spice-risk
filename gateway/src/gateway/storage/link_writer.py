"""Append-only canonical writer for link-quality snapshots."""

from __future__ import annotations

from pathlib import Path

from gateway.link.stats import LinkSnapshot
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.raw_writer import CsvAppendWriter
from gateway.storage.schema import LINK_QUALITY_COLUMNS
from gateway.utils.timeutils import parse_utc_iso


class LinkQualityWriter:
    """Persist link metrics snapshots into daily CSV partitions."""

    def __init__(self, data_root: Path | str | None = None) -> None:
        self.paths: StoragePaths = build_storage_paths(data_root)
        self.paths.ensure_base_dirs()
        self._writers: dict[Path, CsvAppendWriter] = {}

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

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()

    def _writer_for(self, path: Path) -> CsvAppendWriter:
        writer = self._writers.get(path)
        if writer is None:
            writer = CsvAppendWriter(path, LINK_QUALITY_COLUMNS)
            self._writers[path] = writer
        return writer
