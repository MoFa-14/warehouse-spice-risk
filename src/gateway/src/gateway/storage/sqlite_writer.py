# File overview:
# - Responsibility: SQLite-backed storage writer and queue pipeline for live gateway
#   ingestion.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""SQLite-backed storage writer and queue pipeline for live gateway ingestion.

This module is the durable landing zone for accepted telemetry and link-quality
snapshots. In the wider architecture it sits between real-time ingestion and
every later analysis stage. If this writer is missing or behaves incorrectly,
the dashboard has no trustworthy history and the forecasting pipeline has no
stable source of truth.

Two layers are defined here:

- ``SqliteStorageWriter`` performs the actual insert operations against one
  SQLite connection and manages per-pod session tracking.
- ``SqliteWriterPipeline`` wraps that writer in an asynchronous queue so
  ingestion tasks can stay responsive even when disk writes are slower than the
  network input.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable

from gateway.link.stats import LinkSnapshot
from gateway.multi.record import TelemetryRecord as MultiTelemetryRecord
from gateway.protocol.decoder import TelemetryRecord as ProtocolTelemetryRecord
from gateway.protocol.validation import format_quality_flags
from gateway.storage.sqlite_db import connect_sqlite, initialize_schema, resolve_db_path
from gateway.utils.sequence import sequence_reset_detected
from gateway.utils.timeutils import utc_now, utc_now_iso


LOGGER = logging.getLogger(__name__)
# Class purpose: Outcome of attempting to insert one telemetry sample row.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class SqliteWriteResult:
    """Outcome of attempting to insert one telemetry sample row."""

    inserted: bool
    duplicate: bool
# Class purpose: Last known persisted sequence state for one pod.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class _PodSessionState:
    """Last known persisted sequence state for one pod."""

    session_id: int = 0
    last_seq: int | None = None
    last_uptime_s: float | None = None
# Class purpose: One telemetry sample waiting to be inserted into SQLite.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class SampleWriteRequest:
    """One telemetry sample waiting to be inserted into SQLite."""

    ts_pc_utc: str
    record: ProtocolTelemetryRecord
    rssi: int | None
    quality_flags: tuple[str, ...]
    source: str = "BLE"
    result: SqliteWriteResult | None = None
    counted_write: bool = False
# Class purpose: One link snapshot waiting to be inserted into SQLite.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class LinkSnapshotWriteRequest:
    """One link snapshot waiting to be inserted into SQLite."""

    snapshot: LinkSnapshot
    counted_write: bool = False
# Class purpose: Operational counters for the SQLite writer queue.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class SqliteWriterMetrics:
    """Operational counters for the SQLite writer queue."""

    rows_written: int = 0
    commits: int = 0
    write_errors: int = 0
    last_write_time_utc: str | None = None
# Class purpose: Own one SQLite connection and persist gateway records through it.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class SqliteStorageWriter:
    """Own one SQLite connection and persist gateway records through it.

    This object knows how to translate the gateway's normalized telemetry model
    into the database schema. It also tracks pod session boundaries so sequence
    numbers can restart cleanly after a firmware reboot or an explicit reset.
    """
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as db_path, connection_factory, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        connection_factory: Callable[[Path | str | None], object] | None = None,
    ) -> None:
        self.db_path = resolve_db_path(db_path)
        self._connection_factory = connection_factory or connect_sqlite
        self._connection = self._open_connection()
        self._session_state_by_pod: dict[str, _PodSessionState] = {}
    # Method purpose: Insert one telemetry sample into ``samples_raw``.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as ts_pc_utc, record, rssi, quality_flags,
    #   source, interpreted according to the rules encoded in the body below.
    # - Outputs: Returns SqliteWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_sample(
        self,
        *,
        ts_pc_utc: str,
        record: ProtocolTelemetryRecord,
        rssi: int | None,
        quality_flags: Iterable[str],
        source: str,
    ) -> SqliteWriteResult:
        """Insert one telemetry sample into ``samples_raw``.

        The writer preserves the original gateway acceptance timestamp, pod
        sequence number, optional values, and any quality flags. Duplicate
        suppression is handled by the database schema, and the return value
        tells the caller whether this insert created a new row or hit an
        existing unique key.
        """
        quality_flags_tuple = tuple(quality_flags)
        # Sequence resets matter because the same pod can reboot and begin again
        # at sequence 1. Persisting a session identifier prevents those later
        # rows from being mistaken for duplicates of older runs.
        force_sequence_reset = any(str(flag).strip().lower() == "sequence_reset" for flag in quality_flags_tuple)
        quality_text = format_quality_flags(quality_flags_tuple)
        state = self._session_state_for(record.pod_id)
        session_id = self._resolve_session_id(state, record, force_sequence_reset=force_sequence_reset)
        cursor = self._connection.execute(
            """
            INSERT OR IGNORE INTO samples_raw (
                ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_pc_utc,
                record.pod_id,
                session_id,
                int(record.seq),
                float(record.ts_uptime_s),
                record.temp_c,
                record.rh_pct,
                int(record.flags),
                rssi,
                quality_text,
                str(source),
            ),
        )
        self._connection.commit()
        inserted = cursor.rowcount == 1
        self._remember_progress(state, record, session_id)
        return SqliteWriteResult(inserted=inserted, duplicate=not inserted)
    # Method purpose: Adapter for the multi-source queue record shape used
    #   elsewhere.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as record, quality_flags, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: Returns SqliteWriteResult when the function completes
    #   successfully.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_record(self, record: MultiTelemetryRecord, *, quality_flags: Iterable[str]) -> SqliteWriteResult:
        """Adapter for the multi-source queue record shape used elsewhere."""
        return self.write_sample(
            ts_pc_utc=record.ts_pc_utc,
            record=ProtocolTelemetryRecord(
                pod_id=record.pod_id,
                seq=record.seq,
                ts_uptime_s=record.ts_uptime_s,
                temp_c=record.temp_c,
                rh_pct=record.rh_pct,
                flags=record.flags,
            ),
            rssi=record.rssi,
            quality_flags=quality_flags,
            source=record.source,
        )
    # Method purpose: Persist one link-health snapshot for later diagnostics and
    #   review.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def write_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        """Persist one link-health snapshot for later diagnostics and review."""
        self._connection.execute(
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
                int(snapshot.total_received),
                int(snapshot.total_missing),
                int(snapshot.total_duplicates),
                int(snapshot.disconnect_count),
                int(snapshot.reconnect_count),
                float(snapshot.missing_rate),
            ),
        )
        self._connection.commit()
    # Method purpose: Implements the log event step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as ts_pc_utc, level, message, pod_id, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def log_event(self, *, ts_pc_utc: str, level: str, message: str, pod_id: str | None = None) -> None:
        self._connection.execute(
            "INSERT INTO gateway_events (ts_pc_utc, level, pod_id, message) VALUES (?, ?, ?, ?)",
            (ts_pc_utc, str(level), pod_id, str(message)),
        )
        self._connection.commit()
    # Method purpose: Implements the close step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._connection.close()
    # Method purpose: Implements the open connection step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _open_connection(self):
        connection = self._connection_factory(self.db_path)
        initialize_schema(connection)
        return connection
    # Method purpose: Load or reuse the last persisted sequence state for one
    #   pod.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns _PodSessionState when the function completes
    #   successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _session_state_for(self, pod_id: str) -> _PodSessionState:
        """Load or reuse the last persisted sequence state for one pod."""
        state = self._session_state_by_pod.get(pod_id)
        if state is not None:
            return state

        row = self._connection.execute(
            """
            SELECT session_id, seq, ts_uptime_s
            FROM samples_raw
            WHERE pod_id = ?
            ORDER BY ts_pc_utc DESC, session_id DESC, seq DESC
            LIMIT 1
            """,
            (str(pod_id),),
        ).fetchone()
        state = _PodSessionState(
            session_id=int(row["session_id"]) if row is not None else 0,
            last_seq=int(row["seq"]) if row is not None else None,
            last_uptime_s=float(row["ts_uptime_s"]) if row is not None and row["ts_uptime_s"] is not None else None,
        )
        self._session_state_by_pod[pod_id] = state
        return state
    # Method purpose: Resolves session identifier into the concrete value used
    #   later.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as state, record, force_sequence_reset,
    #   interpreted according to the rules encoded in the body below.
    # - Outputs: Returns int when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @staticmethod
    def _resolve_session_id(
        state: _PodSessionState,
        record: ProtocolTelemetryRecord,
        *,
        force_sequence_reset: bool = False,
    ) -> int:
        if state.last_seq is None:
            return state.session_id
        if force_sequence_reset or SqliteStorageWriter._is_sequence_reset(state, record):
            return state.session_id + 1
        return state.session_id
    # Method purpose: Implements the is sequence reset step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as state, record, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @staticmethod
    def _is_sequence_reset(state: _PodSessionState, record: ProtocolTelemetryRecord) -> bool:
        return sequence_reset_detected(
            last_seq=state.last_seq,
            last_uptime_s=state.last_uptime_s,
            seq=int(record.seq),
            ts_uptime_s=float(record.ts_uptime_s),
        )
    # Method purpose: Implements the remember progress step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteStorageWriter.
    # - Inputs: Arguments such as state, record, session_id, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @staticmethod
    def _remember_progress(state: _PodSessionState, record: ProtocolTelemetryRecord, session_id: int) -> None:
        if session_id != state.session_id:
            state.session_id = session_id
            state.last_seq = int(record.seq)
            state.last_uptime_s = float(record.ts_uptime_s)
            return

        if state.last_seq is None or int(record.seq) > state.last_seq:
            state.last_seq = int(record.seq)
        if state.last_uptime_s is None or float(record.ts_uptime_s) > state.last_uptime_s:
            state.last_uptime_s = float(record.ts_uptime_s)
# Class purpose: Factory bundle used by tests and the runtime to create SQLite
#   writers.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass
class SqliteWriterDependencies:
    """Factory bundle used by tests and the runtime to create SQLite writers."""

    storage_writer_factory: Callable[[Path], SqliteStorageWriter] = SqliteStorageWriter
# Class purpose: Asynchronous queue-backed persistence pipeline.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

class SqliteWriterPipeline:
    """Asynchronous queue-backed persistence pipeline.

    The gateway should keep reading telemetry even if an individual disk write
    stalls or temporarily fails. This wrapper therefore separates ingestion from
    persistence with a queue and a dedicated consumer task. It also emits
    heartbeat logs so long-running demos can prove that storage is still alive.
    """
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as db_path, queue_maxsize, heartbeat_interval_s,
    #   reopen_delay_s, red_flag_failures_per_minute, dependencies, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def __init__(
        self,
        *,
        db_path: Path | str | None = None,
        queue_maxsize: int = 1000,
        heartbeat_interval_s: float = 10.0,
        reopen_delay_s: float = 0.5,
        red_flag_failures_per_minute: int = 5,
        dependencies: SqliteWriterDependencies | None = None,
    ) -> None:
        self.db_path = resolve_db_path(db_path)
        self.queue: asyncio.Queue[SampleWriteRequest | LinkSnapshotWriteRequest] = asyncio.Queue(maxsize=queue_maxsize)
        self.heartbeat_interval_s = heartbeat_interval_s
        self.reopen_delay_s = reopen_delay_s
        self.red_flag_failures_per_minute = red_flag_failures_per_minute
        self.metrics = SqliteWriterMetrics()
        self._dependencies = dependencies or SqliteWriterDependencies()
        self._storage_writer: SqliteStorageWriter | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._error_times: deque[datetime] = deque()
        self._last_red_flag_at: datetime | None = None
    # Method purpose: Start the background consumer and heartbeat tasks.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def start(self) -> None:
        """Start the background consumer and heartbeat tasks."""
        if self._consumer_task is not None:
            return
        self._consumer_task = asyncio.create_task(self._consumer_loop(), name="sqlite-writer-consumer")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="sqlite-writer-heartbeat")
        self._attach_done_logger(self._consumer_task)
        self._attach_done_logger(self._heartbeat_task)
    # Method purpose: Implements the enqueue sample step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as ts_pc_utc, record, rssi, quality_flags,
    #   interpreted according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    async def enqueue_sample(
        self,
        *,
        ts_pc_utc: str,
        record: ProtocolTelemetryRecord,
        rssi: int | None,
        quality_flags: tuple[str, ...],
    ) -> None:
        await self.queue.put(
            SampleWriteRequest(
                ts_pc_utc=ts_pc_utc,
                record=record,
                rssi=rssi,
                quality_flags=tuple(quality_flags),
                source="BLE",
            )
        )
    # Method purpose: Implements the enqueue link snapshot step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as snapshot, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    async def enqueue_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        await self.queue.put(LinkSnapshotWriteRequest(snapshot=snapshot))
    # Method purpose: Implements the stop step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    async def stop(self) -> None:
        self._stop_event.set()
        await self.queue.join()

        if self._consumer_task is not None:
            await self.queue.put(LinkSnapshotWriteRequest(snapshot=_stop_snapshot()))
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

        self._close_storage_writer()
    # Method purpose: Drain queued items and retry safely if SQLite becomes
    #   unavailable.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    async def _consumer_loop(self) -> None:
        """Drain queued items and retry safely if SQLite becomes unavailable.

        A failed write does not discard the pending item. The item remains
        pending, the connection is reopened, and the loop retries after a short
        backoff. This is important because transient file-lock or I/O issues
        should not silently lose telemetry.
        """
        pending_item: SampleWriteRequest | LinkSnapshotWriteRequest | None = None

        while True:
            if pending_item is None:
                pending_item = await self.queue.get()

            if _is_stop_item(pending_item):
                self.queue.task_done()
                return

            try:
                self._process_item(pending_item)
            except Exception:
                self._record_error()
                LOGGER.exception("SQLite writer error while persisting queued item; closing connection and retrying.")
                self._close_storage_writer()
                await asyncio.sleep(self.reopen_delay_s)
                continue

            self.queue.task_done()
            pending_item = None
    # Method purpose: Implements the heartbeat loop step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.heartbeat_interval_s)
            except asyncio.TimeoutError:
                LOGGER.info(
                    "sqlite writer alive rows_written=%s commits=%s write_errors=%s queue_size=%s last_write=%s",
                    self.metrics.rows_written,
                    self.metrics.commits,
                    self.metrics.write_errors,
                    self.queue.qsize(),
                    self.metrics.last_write_time_utc or "never",
                )
    # Method purpose: Dispatch queued work to the sample or link-quality path.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as item, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _process_item(self, item: SampleWriteRequest | LinkSnapshotWriteRequest) -> None:
        """Dispatch queued work to the sample or link-quality path."""
        self._ensure_storage_writer()
        if isinstance(item, SampleWriteRequest):
            self._process_sample(item)
            return
        self._process_link_snapshot(item)
    # Method purpose: Implements the process sample step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as item, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _process_sample(self, item: SampleWriteRequest) -> None:
        assert self._storage_writer is not None
        if item.result is None:
            item.result = self._storage_writer.write_sample(
                ts_pc_utc=item.ts_pc_utc,
                record=item.record,
                rssi=item.rssi,
                quality_flags=item.quality_flags,
                source=item.source,
            )
            self.metrics.commits += 1

        if item.result.duplicate:
            return

        if not item.counted_write:
            self._record_success()
            item.counted_write = True
    # Method purpose: Implements the process link snapshot step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as item, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _process_link_snapshot(self, item: LinkSnapshotWriteRequest) -> None:
        if _is_stop_item(item):
            return

        assert self._storage_writer is not None
        self._storage_writer.write_link_snapshot(item.snapshot)
        self.metrics.commits += 1

        if not item.counted_write:
            self._record_success()
            item.counted_write = True
    # Method purpose: Ensures that storage writer exists before later logic
    #   depends on it.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _ensure_storage_writer(self) -> None:
        if self._storage_writer is None:
            self._storage_writer = self._dependencies.storage_writer_factory(self.db_path)
    # Method purpose: Implements the close storage writer step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _close_storage_writer(self) -> None:
        if self._storage_writer is not None:
            with contextlib.suppress(Exception):
                self._storage_writer.close()
        self._storage_writer = None
    # Method purpose: Update error metrics and emit a red-flag log on repeated
    #   failures.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _record_error(self) -> None:
        """Update error metrics and emit a red-flag log on repeated failures."""
        self.metrics.write_errors += 1
        now = utc_now()
        cutoff = now - timedelta(minutes=1)
        self._error_times.append(now)
        while self._error_times and self._error_times[0] < cutoff:
            self._error_times.popleft()

        if len(self._error_times) < self.red_flag_failures_per_minute:
            return

        if self._last_red_flag_at is not None and now - self._last_red_flag_at < timedelta(seconds=30):
            return

        self._last_red_flag_at = now
        LOGGER.error(
            "SQLITE WRITER RED FLAG: %s write failures in the last 60s; still retrying and keeping the queue alive.",
            len(self._error_times),
        )
    # Method purpose: Implements the record success step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def _record_success(self) -> None:
        self.metrics.rows_written += 1
        self.metrics.last_write_time_utc = utc_now_iso()
    # Method purpose: Implements the attach done logger step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on SqliteWriterPipeline.
    # - Inputs: Arguments such as task, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @staticmethod
    def _attach_done_logger(task: asyncio.Task[None]) -> None:
        # Method purpose: Implements the log done step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway persistence layer and acts
        #   as a method on SqliteWriterPipeline.
        # - Inputs: Arguments such as completed, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: Persistence code matters because the rest
        #   of the project only sees what this layer records and exposes.
        # - Related flow: Receives normalized gateway records and passes
        #   stored evidence to forecasting and dashboard loaders.

        def _log_done(completed: asyncio.Task[None]) -> None:
            if completed.cancelled():
                return
            try:
                completed.result()
            except Exception as exc:
                LOGGER.critical(
                    "SQLite writer task %s terminated unexpectedly.",
                    completed.get_name(),
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_log_done)
# Function purpose: Implements the stop snapshot step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns LinkSnapshot when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _stop_snapshot() -> LinkSnapshot:
    return LinkSnapshot(
        ts_pc_utc="1970-01-01T00:00:00Z",
        pod_id="__STOP__",
        connected=False,
        last_rssi=None,
        total_received=0,
        total_missing=0,
        total_duplicates=0,
        disconnect_count=0,
        reconnect_count=0,
        missing_rate=0.0,
    )
# Function purpose: Implements the is stop item step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as item, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _is_stop_item(item: SampleWriteRequest | LinkSnapshotWriteRequest) -> bool:
    return isinstance(item, LinkSnapshotWriteRequest) and item.snapshot.pod_id == "__STOP__"
