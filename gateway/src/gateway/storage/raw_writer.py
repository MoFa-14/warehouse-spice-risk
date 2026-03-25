"""Append-only raw telemetry writer with file-backed dedupe."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.paths import StoragePaths, build_storage_paths
from gateway.storage.schema import RAW_SAMPLE_COLUMNS, QualityFlag, has_quality_flag, parse_quality_mask, quality_flags_to_mask
from gateway.utils.timeutils import parse_utc_iso


class CsvAppendWriter:
    """Small line-buffered CSV append helper shared across Layer 3 writers."""

    def __init__(self, path: Path, fieldnames: list[str]) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists() and self.path.stat().st_size > 0
        self._handle = self.path.open("a", encoding="utf-8", newline="", buffering=1)
        self._writer = csv.DictWriter(self._handle, fieldnames=fieldnames)
        if not file_exists:
            self._writer.writeheader()
            self._handle.flush()

    def write_row(self, row: Mapping[str, Any]) -> None:
        serializable = {key: ("" if value is None else value) for key, value in row.items()}
        self._writer.writerow(serializable)
        self._handle.flush()

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.flush()
            self._handle.close()


@dataclass(frozen=True)
class RawWriteResult:
    """Outcome of attempting to append a raw sample row."""

    inserted: bool
    duplicate: bool
    path: Path


class RawTelemetryWriter:
    """Persist canonical raw telemetry rows into per-pod, per-day CSV files."""

    def __init__(self, data_root: Path | str | None = None) -> None:
        self.paths: StoragePaths = build_storage_paths(data_root)
        self.paths.ensure_base_dirs()
        self._writers: dict[Path, CsvAppendWriter] = {}
        self._seen_by_path: dict[Path, set[int]] = {}

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
        seen_sequences = self._load_seen_sequences(path)
        quality_mask = quality_flags_to_mask(quality_flags)

        if has_quality_flag(quality_mask, QualityFlag.SEQUENCE_RESET):
            seen_sequences.clear()

        if record.seq in seen_sequences:
            return RawWriteResult(inserted=False, duplicate=True, path=path)

        self._writer_for(path).write_row(
            {
                "ts_pc_utc": ts_pc_utc,
                "pod_id": record.pod_id,
                "seq": record.seq,
                "ts_uptime_s": record.ts_uptime_s,
                "temp_c": record.temp_c,
                "rh_pct": record.rh_pct,
                "flags": record.flags,
                "rssi": rssi,
                "quality_flags": quality_mask,
            }
        )
        seen_sequences.add(record.seq)
        return RawWriteResult(inserted=True, duplicate=False, path=path)

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()

    def _writer_for(self, path: Path) -> CsvAppendWriter:
        writer = self._writers.get(path)
        if writer is None:
            writer = CsvAppendWriter(path, RAW_SAMPLE_COLUMNS)
            self._writers[path] = writer
        return writer

    def _load_seen_sequences(self, path: Path) -> set[int]:
        seen = self._seen_by_path.get(path)
        if seen is not None:
            return seen

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
