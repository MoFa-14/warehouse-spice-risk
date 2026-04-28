# File overview:
# - Responsibility: Backfill historical CSV telemetry into the primary SQLite
#   database.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Backfill historical CSV telemetry into the primary SQLite database."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from gateway.link.stats import LinkSnapshot
from gateway.storage.paths import build_storage_paths
from gateway.storage.schema import parse_quality_mask, quality_mask_to_flags
from gateway.protocol.validation import format_quality_flags
from gateway.storage.sqlite_db import connect_sqlite, initialize_schema, resolve_db_path
# Class purpose: Summary of a CSV-to-SQLite backfill run.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class CsvImportResult:
    """Summary of a CSV-to-SQLite backfill run."""

    sample_rows_seen: int = 0
    sample_rows_inserted: int = 0
    sample_duplicates: int = 0
    sample_rows_skipped: int = 0
    link_rows_seen: int = 0
    link_rows_inserted: int = 0
    link_duplicates: int = 0
    link_rows_skipped: int = 0
# Class purpose: One historical sample row parsed from CSV.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class _ImportedSample:
    """One historical sample row parsed from CSV."""

    ts_pc_utc: str
    pod_id: str
    seq: int
    ts_uptime_s: float
    temp_c: float | None
    rh_pct: float | None
    flags: int
    rssi: int | None
    quality_flags: tuple[str, ...]
    sort_index: int
    # Method purpose: Implements the identity key step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on _ImportedSample.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns tuple[str, str, int, float] when the function completes
    #   successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def identity_key(self) -> tuple[str, str, int, float]:
        return (self.pod_id, self.ts_pc_utc, self.seq, round(self.ts_uptime_s, 6))
# Class purpose: One historical link-quality row parsed from CSV.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class _ImportedLinkSnapshot:
    """One historical link-quality row parsed from CSV."""

    snapshot: LinkSnapshot
    sort_index: int
    # Method purpose: Implements the identity key step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on _ImportedLinkSnapshot.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns tuple[str, str, int, int | None, int, int, int, int,
    #   int, float] when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def identity_key(self) -> tuple[str, str, int, int | None, int, int, int, int, int, float]:
        row = self.snapshot
        return (
            row.ts_pc_utc,
            row.pod_id,
            1 if row.connected else 0,
            row.last_rssi,
            row.total_received,
            row.total_missing,
            row.total_duplicates,
            row.disconnect_count,
            row.reconnect_count,
            round(row.missing_rate, 6),
        )
# Function purpose: Copy historical CSV storage into SQLite without modifying the
#   original files.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as data_root, db_path, include_link_quality,
#   include_legacy_logs, pod_ids, interpreted according to the rules encoded in the
#   body below.
# - Outputs: Returns CsvImportResult when the function completes successfully.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def import_csv_history(
    *,
    data_root: Path | str | None = None,
    db_path: Path | str | None = None,
    include_link_quality: bool = True,
    include_legacy_logs: bool = True,
    pod_ids: Iterable[str] | None = None,
) -> CsvImportResult:
    """Copy historical CSV storage into SQLite without modifying the original files."""
    storage_paths = build_storage_paths(data_root)
    resolved_db_path = resolve_db_path(db_path)
    normalized_pod_filter = {
        _normalize_pod_id(pod_id)
        for pod_id in pod_ids or ()
        if str(pod_id).strip()
    }
    sample_rows, skipped_sample_rows = _load_sample_rows(
        storage_paths=storage_paths,
        include_legacy_logs=include_legacy_logs,
        pod_filter=normalized_pod_filter or None,
    )
    if include_link_quality:
        link_rows, skipped_link_rows = _load_link_rows(
            storage_paths=storage_paths,
            include_legacy_logs=include_legacy_logs,
            pod_filter=normalized_pod_filter or None,
        )
    else:
        link_rows, skipped_link_rows = [], 0

    connection = connect_sqlite(resolved_db_path)
    initialize_schema(connection)
    result = CsvImportResult(
        sample_rows_skipped=skipped_sample_rows,
        link_rows_skipped=skipped_link_rows if include_link_quality else 0,
    )
    try:
        existing_sample_keys = _load_existing_sample_keys(connection)
        existing_link_keys = _load_existing_link_keys(connection) if include_link_quality else set()
        session_plan = _plan_sample_sessions(
            sample_rows=sample_rows,
            existing_sample_keys=existing_sample_keys,
            minimum_session_by_pod=_load_minimum_session_ids(connection),
        )

        for sample in sample_rows:
            result.sample_rows_seen += 1
            if sample.identity_key in existing_sample_keys:
                result.sample_duplicates += 1
                continue

            inserted = _insert_sample(
                connection=connection,
                sample=sample,
                session_id=session_plan[sample.identity_key],
            )
            if inserted:
                existing_sample_keys.add(sample.identity_key)
                result.sample_rows_inserted += 1
            else:
                result.sample_duplicates += 1

        for link_row in link_rows:
            result.link_rows_seen += 1
            if link_row.identity_key in existing_link_keys:
                result.link_duplicates += 1
                continue
            if _insert_link_snapshot(connection=connection, snapshot=link_row.snapshot):
                existing_link_keys.add(link_row.identity_key)
                result.link_rows_inserted += 1
            else:
                result.link_duplicates += 1
    finally:
        connection.close()

    return result
# Function purpose: Loads sample rows into the structure expected by downstream
#   code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as storage_paths, include_legacy_logs, pod_filter,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns tuple[list[_ImportedSample], int] when the function completes
#   successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _load_sample_rows(
    *,
    storage_paths,
    include_legacy_logs: bool,
    pod_filter: set[str] | None,
) -> tuple[list[_ImportedSample], int]:
    rows: list[_ImportedSample] = []
    skipped = 0
    sort_index = 0
    for path in _sample_csv_paths(storage_paths=storage_paths, include_legacy_logs=include_legacy_logs):
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw_row in reader:
                try:
                    sample = _parse_sample_row(raw_row=raw_row, sort_index=sort_index)
                except (TypeError, ValueError):
                    skipped += 1
                    sort_index += 1
                    continue
                sort_index += 1
                if pod_filter is not None and sample.pod_id not in pod_filter:
                    continue
                rows.append(sample)

    rows.sort(key=lambda row: (row.pod_id, row.ts_pc_utc, row.ts_uptime_s, row.seq, row.sort_index))
    return rows, skipped
# Function purpose: Loads link rows into the structure expected by downstream code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as storage_paths, include_legacy_logs, pod_filter,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns tuple[list[_ImportedLinkSnapshot], int] when the function
#   completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _load_link_rows(
    *,
    storage_paths,
    include_legacy_logs: bool,
    pod_filter: set[str] | None,
) -> tuple[list[_ImportedLinkSnapshot], int]:
    rows: list[_ImportedLinkSnapshot] = []
    skipped = 0
    sort_index = 0
    for path in _link_csv_paths(storage_paths=storage_paths, include_legacy_logs=include_legacy_logs):
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw_row in reader:
                try:
                    row = _parse_link_row(raw_row=raw_row, sort_index=sort_index)
                except (TypeError, ValueError):
                    skipped += 1
                    sort_index += 1
                    continue
                sort_index += 1
                if pod_filter is not None and row.snapshot.pod_id not in pod_filter:
                    continue
                rows.append(row)

    rows.sort(key=lambda row: (row.snapshot.pod_id, row.snapshot.ts_pc_utc, row.sort_index))
    return rows, skipped
# Function purpose: Implements the sample CSV paths step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as storage_paths, include_legacy_logs, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[Path] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _sample_csv_paths(*, storage_paths, include_legacy_logs: bool) -> list[Path]:
    paths = sorted(storage_paths.raw_pods_root.glob("*/*.csv"))
    if include_legacy_logs:
        legacy_samples = storage_paths.root.parent / "gateway" / "logs" / "samples.csv"
        if legacy_samples.exists():
            paths.append(legacy_samples)
    return paths
# Function purpose: Implements the link CSV paths step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as storage_paths, include_legacy_logs, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns list[Path] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _link_csv_paths(*, storage_paths, include_legacy_logs: bool) -> list[Path]:
    paths = sorted(storage_paths.raw_link_quality_root.glob("*.csv"))
    if include_legacy_logs:
        legacy_link_quality = storage_paths.root.parent / "gateway" / "logs" / "link_quality.csv"
        if legacy_link_quality.exists():
            paths.append(legacy_link_quality)
    return paths
# Function purpose: Parses sample row into structured values.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as raw_row, sort_index, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns _ImportedSample when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _parse_sample_row(*, raw_row: dict[str, str], sort_index: int) -> _ImportedSample:
    ts_pc_utc = str(raw_row["ts_pc_utc"]).strip()
    pod_id = _normalize_pod_id(raw_row["pod_id"])
    seq = int(str(raw_row["seq"]).strip())
    ts_uptime_s = float(str(raw_row["ts_uptime_s"]).strip())
    temp_c = _parse_optional_float(raw_row.get("temp_c"))
    rh_pct = _parse_optional_float(raw_row.get("rh_pct"))
    flags = int(str(raw_row.get("flags", "0") or "0").strip())
    rssi = _parse_optional_int(raw_row.get("rssi"))
    quality_mask = parse_quality_mask(raw_row.get("quality_flags"))
    return _ImportedSample(
        ts_pc_utc=ts_pc_utc,
        pod_id=pod_id,
        seq=seq,
        ts_uptime_s=ts_uptime_s,
        temp_c=temp_c,
        rh_pct=rh_pct,
        flags=flags,
        rssi=rssi,
        quality_flags=quality_mask_to_flags(quality_mask),
        sort_index=sort_index,
    )
# Function purpose: Parses link row into structured values.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as raw_row, sort_index, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns _ImportedLinkSnapshot when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _parse_link_row(*, raw_row: dict[str, str], sort_index: int) -> _ImportedLinkSnapshot:
    snapshot = LinkSnapshot(
        ts_pc_utc=str(raw_row["ts_pc_utc"]).strip(),
        pod_id=_normalize_pod_id(raw_row["pod_id"]),
        connected=_parse_boolish(raw_row.get("connected")),
        last_rssi=_parse_optional_int(raw_row.get("last_rssi")),
        total_received=int(str(raw_row.get("total_received", "0") or "0").strip()),
        total_missing=int(str(raw_row.get("total_missing", "0") or "0").strip()),
        total_duplicates=int(str(raw_row.get("total_duplicates", "0") or "0").strip()),
        disconnect_count=int(str(raw_row.get("disconnect_count", "0") or "0").strip()),
        reconnect_count=int(str(raw_row.get("reconnect_count", "0") or "0").strip()),
        missing_rate=float(str(raw_row.get("missing_rate", "0") or "0").strip()),
    )
    return _ImportedLinkSnapshot(snapshot=snapshot, sort_index=sort_index)
# Function purpose: Loads existing sample keys into the structure expected by
#   downstream code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns set[tuple[str, str, int, float]] when the function completes
#   successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _load_existing_sample_keys(connection) -> set[tuple[str, str, int, float]]:
    return {
        (
            str(row["pod_id"]),
            str(row["ts_pc_utc"]),
            int(row["seq"]),
            round(float(row["ts_uptime_s"] or 0.0), 6),
        )
        for row in connection.execute(
            "SELECT pod_id, ts_pc_utc, seq, ts_uptime_s FROM samples_raw"
        ).fetchall()
    }
# Function purpose: Loads existing link keys into the structure expected by
#   downstream code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns set[tuple[str, str, int, int | None, int, int, int, int, int,
#   float]] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _load_existing_link_keys(
    connection,
) -> set[tuple[str, str, int, int | None, int, int, int, int, int, float]]:
    return {
        (
            str(row["ts_pc_utc"]),
            _normalize_pod_id(row["pod_id"]),
            int(row["connected"] or 0),
            _parse_optional_int(row["last_rssi"]),
            int(row["total_received"] or 0),
            int(row["total_missing"] or 0),
            int(row["total_duplicates"] or 0),
            int(row["disconnect_count"] or 0),
            int(row["reconnect_count"] or 0),
            round(float(row["missing_rate"] or 0.0), 6),
        )
        for row in connection.execute(
            """
            SELECT ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
                   total_duplicates, disconnect_count, reconnect_count, missing_rate
            FROM link_quality
            """
        ).fetchall()
    }
# Function purpose: Loads minimum session ids into the structure expected by
#   downstream code.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns dict[str, int] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _load_minimum_session_ids(connection) -> dict[str, int]:
    return {
        str(row["pod_id"]): int(row["minimum_session_id"])
        for row in connection.execute(
            "SELECT pod_id, MIN(session_id) AS minimum_session_id FROM samples_raw GROUP BY pod_id"
        ).fetchall()
    }
# Function purpose: Implements the plan sample sessions step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as sample_rows, existing_sample_keys,
#   minimum_session_by_pod, interpreted according to the rules encoded in the body
#   below.
# - Outputs: Returns dict[tuple[str, str, int, float], int] when the function
#   completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _plan_sample_sessions(
    *,
    sample_rows: list[_ImportedSample],
    existing_sample_keys: set[tuple[str, str, int, float]],
    minimum_session_by_pod: dict[str, int],
) -> dict[tuple[str, str, int, float], int]:
    planned_by_key: dict[tuple[str, str, int, float], int] = {}
    grouped_rows: dict[str, list[_ImportedSample]] = {}
    for row in sample_rows:
        if row.identity_key in existing_sample_keys or row.identity_key in planned_by_key:
            continue
        grouped_rows.setdefault(row.pod_id, []).append(row)

    for pod_id, rows in grouped_rows.items():
        sessions: list[list[_ImportedSample]] = []
        current_session: list[_ImportedSample] = []
        previous_row: _ImportedSample | None = None
        for row in rows:
            if previous_row is None or not _is_session_reset(previous_row, row):
                current_session.append(row)
            else:
                sessions.append(current_session)
                current_session = [row]
            previous_row = row
        if current_session:
            sessions.append(current_session)

        minimum_session_id = minimum_session_by_pod.get(pod_id, 0)
        first_session_id = min(minimum_session_id, 0) - len(sessions)

        for index, session_rows in enumerate(sessions):
            session_id = first_session_id + index
            for row in session_rows:
                planned_by_key[row.identity_key] = session_id

    return planned_by_key
# Function purpose: Implements the is session reset step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as previous_row, current_row, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _is_session_reset(previous_row: _ImportedSample, current_row: _ImportedSample) -> bool:
    if current_row.ts_uptime_s + 1.0 < previous_row.ts_uptime_s:
        return True
    if current_row.seq == 1 and previous_row.seq > 1:
        return True
    return False
# Function purpose: Implements the insert sample step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, sample, session_id, interpreted according
#   to the rules encoded in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _insert_sample(*, connection, sample: _ImportedSample, session_id: int) -> bool:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO samples_raw (
            ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sample.ts_pc_utc,
            sample.pod_id,
            session_id,
            sample.seq,
            sample.ts_uptime_s,
            sample.temp_c,
            sample.rh_pct,
            sample.flags,
            sample.rssi,
            format_quality_flags(sample.quality_flags),
            "CSV_BACKFILL",
        ),
    )
    connection.commit()
    return cursor.rowcount == 1
# Function purpose: Implements the insert link snapshot step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as connection, snapshot, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _insert_link_snapshot(*, connection, snapshot: LinkSnapshot) -> bool:
    cursor = connection.execute(
        """
        INSERT INTO link_quality (
            ts_pc_utc, pod_id, connected, last_rssi, total_received, total_missing,
            total_duplicates, disconnect_count, reconnect_count, missing_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot.ts_pc_utc,
            snapshot.pod_id,
            1 if snapshot.connected else 0,
            snapshot.last_rssi,
            snapshot.total_received,
            snapshot.total_missing,
            snapshot.total_duplicates,
            snapshot.disconnect_count,
            snapshot.reconnect_count,
            snapshot.missing_rate,
        ),
    )
    connection.commit()
    return cursor.rowcount == 1
# Function purpose: Normalizes pod identifier into the subsystem's stable
#   representation.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _normalize_pod_id(value: object) -> str:
    text = str(value).strip()
    if text.upper().startswith("SHT45-POD-"):
        return text.rsplit("-", maxsplit=1)[-1]
    return text
# Function purpose: Parses optional float into structured values.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns float | None when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _parse_optional_float(value: object) -> float | None:
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return float(text)
# Function purpose: Parses optional int into structured values.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns int | None when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _parse_optional_int(value: object) -> int | None:
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return int(float(text))
# Function purpose: Parses boolish into structured values.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _parse_boolish(value: object) -> bool:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")
