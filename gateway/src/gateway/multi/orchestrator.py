"""Top-level runtime orchestration for concurrent BLE and TCP ingestion."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from pathlib import Path

from gateway.config import ValidationSettings
from gateway.firmware_config_loader import FirmwareConfig, default_firmware_config_path, load_firmware_config
from gateway.ingesters.ble_ingester import BleIngester, BleIngesterSettings
from gateway.ingesters.tcp_ingester import TcpIngester, TcpIngesterSettings
from gateway.logging.process_lock import GatewayProcessLock, build_lock_path
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter
from gateway.storage.sqlite_db import resolve_db_path
from gateway.utils.timeutils import utc_now_iso


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MultiGatewaySettings:
    """Runtime settings for the multi-pod gateway mode."""

    firmware_config_path: str | None
    ble_addresses: tuple[str, ...]
    ble_name_prefix: str
    tcp_port: int
    duration_s: float | None
    log_root: Path
    interval_s: int
    storage_backend: str = "sqlite"
    db_path: Path | None = None
    temp_min_c: float = -20.0
    temp_max_c: float = 80.0
    scan_timeout_s: float = 10.0
    rssi_poll_interval_s: float = 30.0
    stats_interval_s: float = 30.0
    use_cached_services: bool = False

    @property
    def firmware(self) -> FirmwareConfig:
        config_path = Path(self.firmware_config_path) if self.firmware_config_path else default_firmware_config_path()
        return load_firmware_config(config_path)

    @property
    def resolved_db_path(self) -> Path:
        return resolve_db_path(self.db_path)


class MultiGatewayOrchestrator:
    """Run concurrent BLE and TCP ingestion into a shared per-pod router."""

    def __init__(self, settings: MultiGatewaySettings) -> None:
        self.settings = settings
        self.queue: asyncio.Queue[TelemetryRecord] = asyncio.Queue(maxsize=1000)
        firmware = settings.firmware
        self.router = PodRouter(
            queue=self.queue,
            firmware=firmware,
            validation=ValidationSettings(temp_min_c=settings.temp_min_c, temp_max_c=settings.temp_max_c),
            storage_backend=settings.storage_backend,
            data_root=settings.log_root.parent,
            db_path=settings.resolved_db_path,
        )
        self.ble_ingester = BleIngester(
            queue=self.queue,
            router=self.router,
            settings=BleIngesterSettings(
                firmware_config_path=settings.firmware_config_path,
                addresses=settings.ble_addresses,
                ble_name_prefix=settings.ble_name_prefix,
                sample_interval_s=settings.interval_s,
                scan_timeout_s=settings.scan_timeout_s,
                rssi_poll_interval_s=settings.rssi_poll_interval_s,
                temp_min_c=settings.temp_min_c,
                temp_max_c=settings.temp_max_c,
                use_cached_services=settings.use_cached_services,
            ),
        )
        self.tcp_ingester = TcpIngester(
            queue=self.queue,
            router=self.router,
            settings=TcpIngesterSettings(port=settings.tcp_port),
        )
        lock_target = self.settings.resolved_db_path if self.settings.storage_backend == "sqlite" else self.settings.log_root
        compatibility_lock_paths = (
            (self.settings.resolved_db_path.parent / ".lock",) if self.settings.storage_backend == "sqlite" else ()
        )
        self.process_lock = GatewayProcessLock(
            build_lock_path(lock_target),
            compatibility_lock_paths=compatibility_lock_paths,
        )
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def run(self) -> int:
        LOGGER.info(
            "Multi-pod gateway starting with BLE prefix=%s tcp_port=%s interval=%ss storage=%s",
            self.settings.ble_name_prefix,
            self.settings.tcp_port,
            self.settings.interval_s,
            self.settings.storage_backend,
        )
        if self.settings.storage_backend == "sqlite":
            LOGGER.info("Primary SQLite DB: %s", self.settings.resolved_db_path)
        else:
            LOGGER.info("Per-pod raw CSV root: %s", self.settings.log_root / "pods")
        try:
            self.process_lock.acquire()
        except RuntimeError as exc:
            LOGGER.error("%s", exc)
            return 1
        try:
            self.router.start()
            await self.tcp_ingester.start()
            await self.ble_ingester.start()

            self._tasks = [
                asyncio.create_task(self.ble_ingester.refresh_rssi_loop(), name="multi-ble-rssi"),
                asyncio.create_task(self._stats_loop(), name="multi-stats-loop"),
            ]
            for task in self._tasks:
                task.add_done_callback(self._handle_task_done)

            if self.settings.duration_s is None:
                await self._stop_event.wait()
            else:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.duration_s)
                except asyncio.TimeoutError:
                    pass
            return 0
        finally:
            self._stop_event.set()
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await self.ble_ingester.stop()
            await self.tcp_ingester.stop()
            with contextlib.suppress(Exception):
                self._write_link_snapshots()
            await self.router.stop()
            self.process_lock.release()

    async def _stats_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.stats_interval_s)
            except asyncio.TimeoutError:
                for snapshot in self.router.stats_snapshot():
                    LOGGER.info(
                        "stats pod=%s source=%s received=%s missing=%s duplicates=%s corrupt_count=%s reconnects=%s missing_rate=%.4f",
                        snapshot.pod_id,
                        snapshot.source,
                        snapshot.received,
                        snapshot.missing,
                        snapshot.duplicates,
                        snapshot.corrupt_count,
                        snapshot.reconnects,
                        snapshot.missing_rate,
                    )
                try:
                    self._write_link_snapshots()
                except Exception as exc:
                    LOGGER.error(
                        "Multi-gateway link snapshot write failed.",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )

    def _handle_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            LOGGER.critical(
                "Multi-gateway background task terminated unexpectedly.",
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            self._stop_event.set()

    def _write_link_snapshots(self) -> None:
        timestamp = utc_now_iso()
        for snapshot in self.router.build_link_snapshots(ts_pc_utc=timestamp):
            self.router.write_link_snapshot(snapshot)
