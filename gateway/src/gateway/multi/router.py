"""Route normalized telemetry records to per-pod storage and resend logic."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from gateway.config import ValidationSettings
from gateway.control.resend import ResendController
from gateway.firmware_config_loader import FirmwareConfig
from gateway.link.stats import LinkSnapshot
from gateway.link.time_alignment import AlignmentState, DEFAULT_DRIFT_THRESHOLD_S, align_sample, reset_alignment
from gateway.multi.record import TelemetryRecord
from gateway.protocol.decoder import TelemetryRecord as ProtocolTelemetryRecord
from gateway.protocol.validation import validate_telemetry
from gateway.storage.per_pod_csv_writer import PerPodCsvWriter
from gateway.storage.sqlite_reader import latest_sample
from gateway.storage.sqlite_writer import SqliteStorageWriter
from gateway.utils.sequence import sequence_reset_detected
from gateway.utils.timeutils import utc_now, utc_now_iso


LOGGER = logging.getLogger(__name__)


@dataclass
class PodStats:
    """Runtime stats tracked per pod across BLE and TCP ingestion."""

    pod_id: str
    source: str
    received: int = 0
    missing: int = 0
    duplicates: int = 0
    corrupt_count: int = 0
    reconnects: int = 0
    connected: bool = False
    last_rssi: int | None = None
    disconnect_count: int = 0
    last_seq_high_water: int | None = None
    last_uptime_s: float | None = None
    last_seen_utc: str | None = None
    seen_sequences: set[int] = field(default_factory=set)
    hydrated_from_storage: bool = False
    last_duplicate_log_at: datetime | None = None
    suppressed_duplicate_logs: int = 0
    last_requested_seq: int | None = None
    last_requested_seq_at: datetime | None = None
    last_requested_from_seq: int | None = None
    last_requested_from_seq_at: datetime | None = None
    drift_anomalies: int = 0
    last_drift_s: float | None = None

    @property
    def missing_rate(self) -> float:
        denominator = self.received + self.missing
        return (self.missing / denominator) if denominator else 0.0


class PodRouter:
    """Single consumer that validates, logs, stores, and requests resends."""

    def __init__(
        self,
        *,
        queue: asyncio.Queue[TelemetryRecord],
        firmware: FirmwareConfig,
        validation: ValidationSettings,
        storage_backend: str = "csv",
        data_root,
        db_path=None,
        reopen_delay_s: float = 0.5,
        resend_cooldown_s: float = 5.0,
        duplicate_log_interval_s: float = 5.0,
    ) -> None:
        self.queue = queue
        self.firmware = firmware
        self.validation = validation
        self.storage_backend = str(storage_backend).strip().lower()
        self.data_root = data_root
        self.db_path = db_path
        self.writer = self._build_writer()
        self.reopen_delay_s = reopen_delay_s
        self.resend_cooldown = timedelta(seconds=max(0.0, resend_cooldown_s))
        self.duplicate_log_interval = timedelta(seconds=max(0.0, duplicate_log_interval_s))
        self._consumer_task: asyncio.Task[None] | None = None
        self._resend_controllers: dict[str, ResendController] = {}
        self._stats: dict[str, PodStats] = {}
        self._alignment_by_pod: dict[str, AlignmentState] = {}

    def start(self) -> None:
        if self._consumer_task is not None:
            return
        self._consumer_task = asyncio.create_task(self._consume_loop(), name="multi-pod-router")
        self._consumer_task.add_done_callback(self._log_task_failure)

    async def stop(self) -> None:
        await self.queue.join()
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None
        self.writer.close()

    def register_resend_controller(self, pod_id: str, controller: ResendController) -> None:
        self._resend_controllers[pod_id] = controller

    def note_reconnect(self, pod_id: str, source: str) -> None:
        self._stats_for(pod_id, source).reconnects += 1

    def note_connected(self, pod_id: str, source: str, *, last_rssi: int | None = None) -> None:
        stats = self._stats_for(pod_id, source)
        was_connected = stats.connected
        stats.connected = True
        if last_rssi is not None:
            stats.last_rssi = int(last_rssi)
        if not was_connected:
            LOGGER.info("[pod=%s source=%s] connected", pod_id, source)

    def note_disconnected(self, pod_id: str, source: str) -> None:
        stats = self._stats_for(pod_id, source)
        was_connected = stats.connected
        if was_connected:
            stats.disconnect_count += 1
        stats.connected = False
        if was_connected:
            LOGGER.info("[pod=%s source=%s] disconnected", pod_id, source)

    def update_rssi(self, pod_id: str, source: str, rssi: int | None) -> None:
        if rssi is None:
            return
        self._stats_for(pod_id, source).last_rssi = int(rssi)

    async def note_corrupt(self, pod_id: str, source: str) -> None:
        stats = self._stats_for(pod_id, source)
        self._hydrate_stats_from_storage(stats)
        stats.corrupt_count += 1
        expected_seq = 1 if stats.last_seq_high_water is None else stats.last_seq_high_water + 1
        await self._request_seq(pod_id, expected_seq, stats=stats)

    def stats_snapshot(self) -> list[PodStats]:
        return [self._clone_stats(item) for item in self._stats.values()]

    def build_link_snapshots(self, *, ts_pc_utc: str) -> list[LinkSnapshot]:
        return [
            LinkSnapshot(
                ts_pc_utc=ts_pc_utc,
                pod_id=stats.pod_id,
                connected=stats.connected,
                last_rssi=stats.last_rssi,
                total_received=stats.received,
                total_missing=stats.missing,
                total_duplicates=stats.duplicates,
                disconnect_count=stats.disconnect_count,
                reconnect_count=stats.reconnects,
                missing_rate=stats.missing_rate,
            )
            for stats in self.stats_snapshot()
        ]

    def write_link_snapshot(self, snapshot: LinkSnapshot) -> None:
        self.writer.write_link_snapshot(snapshot)

    async def _consume_loop(self) -> None:
        while True:
            record = await self.queue.get()
            try:
                await self._process_record(record)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Multi-pod router failed while processing pod=%s seq=%s; retrying writer.", record.pod_id, record.seq)
                self.writer.close()
                await asyncio.sleep(self.reopen_delay_s)
                self.writer = self._build_writer()
                await self._process_record(record)
            finally:
                self.queue.task_done()

    async def _process_record(self, record: TelemetryRecord) -> None:
        stats = self._stats_for(record.pod_id, record.source)
        self._hydrate_stats_from_storage(stats)
        quality_flags = self._quality_flags(record)

        if self._should_reset_sequence(stats, record):
            stats.seen_sequences.clear()
            stats.last_seq_high_water = None
            stats.last_uptime_s = None
            quality_flags.append("sequence_reset")
            reset_alignment(self._alignment_state_for(record.pod_id))

        if record.seq in stats.seen_sequences:
            stats.duplicates += 1
            self._log_duplicate(stats, record, reason="already seen in this run")
            return

        if stats.last_seq_high_water is not None and record.seq > stats.last_seq_high_water + 1:
            missing_from = stats.last_seq_high_water + 1
            stats.missing += record.seq - stats.last_seq_high_water - 1
            quality_flags.append("seq_gap")
            await self._request_from_seq(record.pod_id, missing_from, stats=stats)

        alignment = align_sample(
            self._alignment_state_for(record.pod_id),
            gateway_ts_utc=record.ts_pc_utc,
            ts_uptime_s=record.ts_uptime_s,
            drift_threshold_s=max(DEFAULT_DRIFT_THRESHOLD_S, float(self.firmware.sample_interval_s * 2)),
        )
        stats.last_drift_s = alignment.drift_s
        if alignment.anomalous:
            stats.drift_anomalies += 1
            quality_flags.append("time_sync_anomaly")
            self._log_gateway_event(
                level="warning",
                pod_id=record.pod_id,
                message=f"time_sync_anomaly drift_s={alignment.drift_s:.1f} seq={record.seq}",
            )

        write_result = self.writer.write_record(record, quality_flags=tuple(dict.fromkeys(quality_flags)))
        if write_result.duplicate:
            stats.duplicates += 1
            self._remember_progress(stats, record)
            self._log_duplicate(stats, record, reason="already stored")
            return

        stats.received += 1
        self._remember_progress(stats, record)

        LOGGER.info(
            "[pod=%s source=%s] seq=%s temp=%s rh=%s flags=%s",
            record.pod_id,
            record.source,
            record.seq,
            record.temp_c,
            record.rh_pct,
            record.flags,
        )

    def _quality_flags(self, record: TelemetryRecord) -> list[str]:
        validated = validate_telemetry(
            ProtocolTelemetryRecord(
                pod_id=record.pod_id,
                seq=record.seq,
                ts_uptime_s=record.ts_uptime_s,
                temp_c=record.temp_c,
                rh_pct=record.rh_pct,
                flags=record.flags,
            ),
            temp_min_c=self.validation.temp_min_c,
            temp_max_c=self.validation.temp_max_c,
            firmware=self.firmware,
        )
        return list(validated.quality_flags)

    def _stats_for(self, pod_id: str, source: str) -> PodStats:
        stats = self._stats.get(pod_id)
        if stats is None:
            stats = PodStats(pod_id=pod_id, source=source)
            self._stats[pod_id] = stats
        else:
            stats.source = source
        return stats

    @staticmethod
    def _should_reset_sequence(stats: PodStats, record: TelemetryRecord) -> bool:
        return sequence_reset_detected(
            last_seq=stats.last_seq_high_water,
            last_uptime_s=stats.last_uptime_s,
            seq=int(record.seq),
            ts_uptime_s=float(record.ts_uptime_s),
        )

    @staticmethod
    def _clone_stats(stats: PodStats) -> PodStats:
        return PodStats(
            pod_id=stats.pod_id,
            source=stats.source,
            received=stats.received,
            missing=stats.missing,
            duplicates=stats.duplicates,
            corrupt_count=stats.corrupt_count,
            reconnects=stats.reconnects,
            connected=stats.connected,
            last_rssi=stats.last_rssi,
            disconnect_count=stats.disconnect_count,
            last_seq_high_water=stats.last_seq_high_water,
            last_uptime_s=stats.last_uptime_s,
            last_seen_utc=stats.last_seen_utc,
            seen_sequences=set(stats.seen_sequences),
            hydrated_from_storage=stats.hydrated_from_storage,
            last_duplicate_log_at=stats.last_duplicate_log_at,
            suppressed_duplicate_logs=stats.suppressed_duplicate_logs,
            last_requested_seq=stats.last_requested_seq,
            last_requested_seq_at=stats.last_requested_seq_at,
            last_requested_from_seq=stats.last_requested_from_seq,
            last_requested_from_seq_at=stats.last_requested_from_seq_at,
            drift_anomalies=stats.drift_anomalies,
            last_drift_s=stats.last_drift_s,
        )

    def _alignment_state_for(self, pod_id: str) -> AlignmentState:
        state = self._alignment_by_pod.get(pod_id)
        if state is None:
            state = AlignmentState()
            self._alignment_by_pod[pod_id] = state
        return state

    def _build_writer(self):
        if self.storage_backend == "sqlite":
            return SqliteStorageWriter(self.db_path)
        return PerPodCsvWriter(self.data_root)

    def _hydrate_stats_from_storage(self, stats: PodStats) -> None:
        if stats.hydrated_from_storage or self.storage_backend != "sqlite":
            return
        row = latest_sample(pod_id=stats.pod_id, db_path=self.db_path)
        if row is not None:
            seq_value = row.get("seq")
            uptime_value = row.get("ts_uptime_s")
            if seq_value is not None:
                stats.last_seq_high_water = int(seq_value)
            if uptime_value is not None:
                stats.last_uptime_s = float(uptime_value)
            ts_value = row.get("ts_pc_utc")
            stats.last_seen_utc = None if ts_value is None else str(ts_value)
        stats.hydrated_from_storage = True

    @staticmethod
    def _remember_progress(stats: PodStats, record: TelemetryRecord) -> None:
        stats.seen_sequences.add(record.seq)
        stats.last_seen_utc = record.ts_pc_utc
        if stats.last_seq_high_water is None or record.seq > stats.last_seq_high_water:
            stats.last_seq_high_water = record.seq
        if stats.last_uptime_s is None or record.ts_uptime_s >= stats.last_uptime_s:
            stats.last_uptime_s = record.ts_uptime_s

    def _log_duplicate(self, stats: PodStats, record: TelemetryRecord, *, reason: str) -> None:
        if not LOGGER.isEnabledFor(logging.DEBUG):
            return
        now = utc_now()
        if (
            stats.last_duplicate_log_at is not None
            and now - stats.last_duplicate_log_at < self.duplicate_log_interval
        ):
            stats.suppressed_duplicate_logs += 1
            return

        suppressed = stats.suppressed_duplicate_logs
        stats.suppressed_duplicate_logs = 0
        stats.last_duplicate_log_at = now
        if suppressed:
            LOGGER.debug(
                "[pod=%s source=%s] duplicate seq=%s ignored (%s, suppressed %s similar duplicate logs)",
                record.pod_id,
                record.source,
                record.seq,
                reason,
                suppressed,
            )
            return
        LOGGER.debug(
            "[pod=%s source=%s] duplicate seq=%s ignored (%s)",
            record.pod_id,
            record.source,
            record.seq,
            reason,
        )

    async def _request_seq(self, pod_id: str, seq: int, *, stats: PodStats) -> None:
        if self._should_skip_seq_request(stats, seq):
            return
        controller = self._resend_controllers.get(pod_id)
        if controller is None:
            LOGGER.warning("No resend controller registered for pod=%s seq=%s", pod_id, seq)
            return
        stats.last_requested_seq = int(seq)
        stats.last_requested_seq_at = utc_now()
        self._log_gateway_event(level="warning", pod_id=pod_id, message=f"resend_request seq={seq}")
        await controller.request_seq(pod_id, seq)

    async def _request_from_seq(self, pod_id: str, from_seq: int, *, stats: PodStats) -> None:
        if self._should_skip_from_seq_request(stats, from_seq):
            return
        controller = self._resend_controllers.get(pod_id)
        if controller is None:
            LOGGER.warning("No resend controller registered for pod=%s from_seq=%s", pod_id, from_seq)
            return
        stats.last_requested_from_seq = int(from_seq)
        stats.last_requested_from_seq_at = utc_now()
        self._log_gateway_event(level="warning", pod_id=pod_id, message=f"resend_request from_seq={from_seq}")
        await controller.request_from_seq(pod_id, from_seq)

    def _should_skip_seq_request(self, stats: PodStats, seq: int) -> bool:
        if stats.last_requested_seq != int(seq) or stats.last_requested_seq_at is None:
            return False
        return utc_now() - stats.last_requested_seq_at < self.resend_cooldown

    def _should_skip_from_seq_request(self, stats: PodStats, from_seq: int) -> bool:
        if stats.last_requested_from_seq != int(from_seq) or stats.last_requested_from_seq_at is None:
            return False
        return utc_now() - stats.last_requested_from_seq_at < self.resend_cooldown

    @staticmethod
    def _log_task_failure(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            LOGGER.critical(
                "Pod router task terminated unexpectedly.",
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    def _log_gateway_event(self, *, level: str, pod_id: str, message: str) -> None:
        log_event = getattr(self.writer, "log_event", None)
        if callable(log_event):
            log_event(ts_pc_utc=utc_now_iso(), level=level, pod_id=pod_id, message=message)
