"""Append-only CSV log writers for samples and link metrics."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Mapping

from gateway.link.stats import LinkSnapshot
from gateway.protocol.decoder import TelemetryRecord
from gateway.protocol.validation import format_quality_flags


SAMPLE_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "seq",
    "ts_uptime_s",
    "temp_c",
    "rh_pct",
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


class CsvAppendLogger:
    """Small append-only CSV logger with line buffering."""

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


class GatewayCsvLogger:
    """Own both gateway CSV outputs and provide typed helper methods."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.samples = CsvAppendLogger(log_dir / "samples.csv", SAMPLE_COLUMNS)
        self.link_quality = CsvAppendLogger(log_dir / "link_quality.csv", LINK_COLUMNS)

    def log_sample(
        self,
        *,
        ts_pc_utc: str,
        record: TelemetryRecord,
        rssi: int | None,
        quality_flags: tuple[str, ...] | list[str],
    ) -> None:
        self.samples.write_row(
            {
                "ts_pc_utc": ts_pc_utc,
                "pod_id": record.pod_id,
                "seq": record.seq,
                "ts_uptime_s": record.ts_uptime_s,
                "temp_c": record.temp_c,
                "rh_pct": record.rh_pct,
                "flags": record.flags,
                "rssi": rssi,
                "quality_flags": format_quality_flags(tuple(quality_flags)),
            }
        )

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

    def close(self) -> None:
        self.samples.close()
        self.link_quality.close()
