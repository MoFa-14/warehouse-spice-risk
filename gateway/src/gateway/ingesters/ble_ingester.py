"""BLE ingestion adapter that feeds normalized records into the multi-pod queue."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass

from gateway.ble.client import PodSession, PodTarget
from gateway.ble.gatt import profile_from_firmware
from gateway.ble.scanner import discover_matches
from gateway.config import GatewaySettings, build_settings
from gateway.control.resend import BleResendController
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter
from gateway.utils.timeutils import utc_now_iso


LOGGER = logging.getLogger(__name__)


def _pod_id_from_name(name: str) -> str:
    parts = str(name).strip().split("-")
    candidate = parts[-1] if parts else name
    return candidate.zfill(2) if candidate.isdigit() else candidate


@dataclass(frozen=True)
class BleIngesterSettings:
    """Configuration for the multi-pod BLE ingestion wrapper."""

    firmware_config_path: str | None
    addresses: tuple[str, ...]
    ble_name_prefix: str
    sample_interval_s: int
    scan_timeout_s: float = 10.0
    rssi_poll_interval_s: float = 30.0
    temp_min_c: float = -20.0
    temp_max_c: float = 80.0
    use_cached_services: bool = False


class BleIngester:
    """Run one or more BLE pod sessions and push normalized records to the queue."""

    def __init__(
        self,
        *,
        queue: asyncio.Queue[TelemetryRecord],
        router: PodRouter,
        settings: BleIngesterSettings,
    ) -> None:
        self.queue = queue
        self.router = router
        self.settings = settings
        self.gateway_settings: GatewaySettings = build_settings(
            firmware_config_path=settings.firmware_config_path,
            log_dir="gateway/logs",
            addresses=list(settings.addresses) if settings.addresses else None,
            scan_timeout_s=settings.scan_timeout_s,
            metrics_interval_s=30.0,
            rssi_poll_interval_s=settings.rssi_poll_interval_s,
            temp_min_c=settings.temp_min_c,
            temp_max_c=settings.temp_max_c,
            send_command=None,
            use_cached_services=settings.use_cached_services,
            ble_name_prefix=settings.ble_name_prefix,
            expected_sample_interval_s=settings.sample_interval_s,
        )
        self.profile = profile_from_firmware(self.gateway_settings.firmware)
        self.sessions: list[PodSession] = []
        self._session_tasks: list[asyncio.Task[None]] = []
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        targets = await self._resolve_targets()
        if not targets:
            LOGGER.warning("No matching BLE pods found for multi-source mode.")
            return

        self.sessions = []
        for target in targets:
            pod_id_hint = _pod_id_from_name(target.name)
            session = PodSession(
                target=target,
                settings=self.gateway_settings,
                profile=self.profile,
                sample_handler=self._make_sample_handler(),
                corrupt_handler=self._make_corrupt_handler(pod_id_hint),
                connect_handler=self._make_connect_handler(pod_id_hint),
                disconnect_handler=self._make_disconnect_handler(pod_id_hint),
            )
            self.router.register_resend_controller(pod_id_hint, BleResendController(session))
            self.sessions.append(session)

        self._session_tasks = [
            asyncio.create_task(session.run(), name=f"ble-pod-session-{index}")
            for index, session in enumerate(self.sessions, start=1)
        ]
        for task in self._session_tasks:
            task.add_done_callback(self._log_task_failure)

    async def stop(self) -> None:
        self._stop_event.set()
        for session in self.sessions:
            await session.stop()
        for task in self._session_tasks:
            task.cancel()
        for task in self._session_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._session_tasks.clear()

    async def refresh_rssi_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.gateway_settings.rssi_poll_interval_s)
            except asyncio.TimeoutError:
                results = await asyncio.gather(
                    *(session.refresh_rssi() for session in self.sessions),
                    return_exceptions=True,
                )
                for session, result in zip(self.sessions, results):
                    if isinstance(result, Exception):
                        LOGGER.debug("RSSI refresh failed for %s: %s", session.target.address, result)
                    else:
                        self.router.update_rssi(_pod_id_from_name(session.target.name or session.target.address), "BLE", session.stats.last_rssi)

    def _make_sample_handler(self):
        async def _handle_sample(record, _quality_flags, stats, timestamp: str) -> None:
            self.router.update_rssi(record.pod_id, "BLE", stats.last_rssi)
            await self.queue.put(
                TelemetryRecord(
                    pod_id=record.pod_id,
                    seq=record.seq,
                    ts_uptime_s=record.ts_uptime_s,
                    temp_c=record.temp_c,
                    rh_pct=record.rh_pct,
                    flags=record.flags,
                    rssi=stats.last_rssi,
                    source="BLE",
                    ts_pc_utc=timestamp or utc_now_iso(),
                )
            )

        return _handle_sample

    def _make_corrupt_handler(self, pod_id: str):
        async def _handle_corrupt() -> None:
            await self.router.note_corrupt(pod_id, "BLE")

        return _handle_corrupt

    def _make_connect_handler(self, pod_id: str):
        async def _handle_connect(is_reconnect: bool) -> None:
            self.router.note_connected(pod_id, "BLE")
            if is_reconnect:
                self.router.note_reconnect(pod_id, "BLE")

        return _handle_connect

    def _make_disconnect_handler(self, pod_id: str):
        async def _handle_disconnect() -> None:
            self.router.note_disconnected(pod_id, "BLE")

        return _handle_disconnect

    async def _resolve_targets(self) -> list[PodTarget]:
        matches = await discover_matches(
            timeout=self.gateway_settings.scan_timeout_s,
            name_prefix=self.gateway_settings.device_name_scan_prefix,
            service_uuid=self.profile.service_uuid,
            addresses=self.gateway_settings.addresses,
        )
        return [PodTarget(address=match.address, name=match.name) for match in matches]

    @staticmethod
    def _log_task_failure(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            LOGGER.critical(
                "BLE ingester task terminated unexpectedly.",
                exc_info=(type(exc), exc, exc.__traceback__),
            )
