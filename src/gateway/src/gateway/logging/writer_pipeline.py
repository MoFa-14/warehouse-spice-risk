"""Queue-backed CSV writer pipeline for gateway telemetry and link snapshots."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from gateway.link.stats import LinkSnapshot
from gateway.logging.csv_logger import GatewayCsvLogger
from gateway.protocol.decoder import TelemetryRecord
from gateway.storage.link_writer import LinkQualityWriter
from gateway.storage.raw_writer import RawTelemetryWriter, RawWriteResult
from gateway.utils.timeutils import utc_now, utc_now_iso


LOGGER = logging.getLogger(__name__)
@dataclass
class SampleWriteRequest:
    """One parsed telemetry record waiting to be persisted."""

    ts_pc_utc: str
    record: TelemetryRecord
    rssi: int | None
    quality_flags: tuple[str, ...]
    canonical_result: RawWriteResult | None = None
    counted_write: bool = False
    legacy_written: bool = False


@dataclass
class LinkSnapshotWriteRequest:
    """One link-quality snapshot waiting to be persisted."""

    snapshot: LinkSnapshot
    canonical_written: bool = False
    counted_write: bool = False
    legacy_written: bool = False


@dataclass
class WriterMetrics:
    """Operational counters for the queue-backed writer."""

    rows_written: int = 0
    write_errors: int = 0
    last_write_time_utc: str | None = None


@dataclass
class WriterDependencies:
    """Factory bundle used by tests and the runtime to create writers."""

    raw_writer_factory: Callable[[Path], RawTelemetryWriter] = RawTelemetryWriter
    link_writer_factory: Callable[[Path], LinkQualityWriter] = LinkQualityWriter
    legacy_logger_factory: Callable[[Path], GatewayCsvLogger] = GatewayCsvLogger


class GatewayWriterPipeline:
    """Serialize all CSV writes through one resilient background task."""

    def __init__(
        self,
        *,
        storage_root: Path,
        log_dir: Path,
        queue_maxsize: int = 1000,
        heartbeat_interval_s: float = 10.0,
        reopen_delay_s: float = 0.5,
        red_flag_failures_per_minute: int = 5,
        dependencies: WriterDependencies | None = None,
    ) -> None:
        self.storage_root = Path(storage_root)
        self.log_dir = Path(log_dir)
        self.queue: asyncio.Queue[SampleWriteRequest | LinkSnapshotWriteRequest] = asyncio.Queue(maxsize=queue_maxsize)
        self.heartbeat_interval_s = heartbeat_interval_s
        self.reopen_delay_s = reopen_delay_s
        self.red_flag_failures_per_minute = red_flag_failures_per_minute
        self.metrics = WriterMetrics()
        self._dependencies = dependencies or WriterDependencies()
        self._raw_writer: RawTelemetryWriter | None = None
        self._link_writer: LinkQualityWriter | None = None
        self._legacy_logger: GatewayCsvLogger | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._error_times: deque[datetime] = deque()
        self._last_red_flag_at: datetime | None = None

    def start(self) -> None:
        """Start the consumer and heartbeat tasks once."""
        if self._consumer_task is not None:
            return
        self._consumer_task = asyncio.create_task(self._consumer_loop(), name="gateway-writer-consumer")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="gateway-writer-heartbeat")
        self._attach_done_logger(self._consumer_task)
        self._attach_done_logger(self._heartbeat_task)

    async def enqueue_sample(
        self,
        *,
        ts_pc_utc: str,
        record: TelemetryRecord,
        rssi: int | None,
        quality_flags: tuple[str, ...],
    ) -> None:
        """Queue one telemetry record for resilient persistence."""
        await self.queue.put(
            SampleWriteRequest(
                ts_pc_utc=ts_pc_utc,
                record=record,
                rssi=rssi,
                quality_flags=quality_flags,
            )
        )

    async def enqueue_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        """Queue one link-quality snapshot for resilient persistence."""
        await self.queue.put(LinkSnapshotWriteRequest(snapshot=snapshot))

    async def stop(self) -> None:
        """Drain queued writes, stop the worker, and close any open file handles."""
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

        self._close_writers()

    async def _consumer_loop(self) -> None:
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
                LOGGER.exception("Writer error while persisting queued CSV item; closing handles and retrying.")
                self._close_writers()
                await asyncio.sleep(self.reopen_delay_s)
                continue

            self.queue.task_done()
            pending_item = None

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.heartbeat_interval_s)
            except asyncio.TimeoutError:
                LOGGER.info(
                    "writer alive rows_written=%s write_errors=%s queue_size=%s last_write=%s",
                    self.metrics.rows_written,
                    self.metrics.write_errors,
                    self.queue.qsize(),
                    self.metrics.last_write_time_utc or "never",
                )

    def _process_item(self, item: SampleWriteRequest | LinkSnapshotWriteRequest) -> None:
        self._ensure_writers()
        if isinstance(item, SampleWriteRequest):
            self._process_sample(item)
            return
        self._process_link_snapshot(item)

    def _process_sample(self, item: SampleWriteRequest) -> None:
        assert self._raw_writer is not None
        if item.canonical_result is None:
            item.canonical_result = self._raw_writer.write_sample(
                ts_pc_utc=item.ts_pc_utc,
                record=item.record,
                rssi=item.rssi,
                quality_flags=item.quality_flags,
            )

        if item.canonical_result.duplicate:
            return

        if not item.counted_write:
            self._record_success()
            item.counted_write = True

        if not item.legacy_written and self._legacy_logger is not None:
            self._legacy_logger.log_sample(
                ts_pc_utc=item.ts_pc_utc,
                record=item.record,
                rssi=item.rssi,
                quality_flags=item.quality_flags,
            )
            item.legacy_written = True

    def _process_link_snapshot(self, item: LinkSnapshotWriteRequest) -> None:
        if _is_stop_item(item):
            return

        assert self._link_writer is not None
        if not item.canonical_written:
            self._link_writer.write_snapshot(item.snapshot)
            item.canonical_written = True

        if not item.counted_write:
            self._record_success()
            item.counted_write = True

        if not item.legacy_written and self._legacy_logger is not None:
            self._legacy_logger.log_link_snapshot(item.snapshot)
            item.legacy_written = True

    def _ensure_writers(self) -> None:
        if self._raw_writer is None:
            self._raw_writer = self._dependencies.raw_writer_factory(self.storage_root)
        if self._link_writer is None:
            self._link_writer = self._dependencies.link_writer_factory(self.storage_root)
        if self._legacy_logger is None:
            self._legacy_logger = self._dependencies.legacy_logger_factory(self.log_dir)

    def _close_writers(self) -> None:
        for writer in (self._legacy_logger, self._link_writer, self._raw_writer):
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()
        self._legacy_logger = None
        self._link_writer = None
        self._raw_writer = None

    def _record_error(self) -> None:
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
            "WRITER RED FLAG: %s write failures in the last 60s; still retrying and keeping the queue alive.",
            len(self._error_times),
        )

    def _record_success(self) -> None:
        self.metrics.rows_written += 1
        self.metrics.last_write_time_utc = utc_now_iso()

    @staticmethod
    def _attach_done_logger(task: asyncio.Task[None]) -> None:
        def _log_done(completed: asyncio.Task[None]) -> None:
            if completed.cancelled():
                return
            try:
                completed.result()
            except Exception as exc:
                LOGGER.critical(
                    "Writer task %s terminated unexpectedly.",
                    completed.get_name(),
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_log_done)


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


def _is_stop_item(item: SampleWriteRequest | LinkSnapshotWriteRequest) -> bool:
    return isinstance(item, LinkSnapshotWriteRequest) and item.snapshot.pod_id == "__STOP__"
