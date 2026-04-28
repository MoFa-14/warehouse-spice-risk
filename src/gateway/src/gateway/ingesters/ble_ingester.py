# File overview:
# - Responsibility: BLE ingestion adapter for physical pods.
# - Project role: Accepts live telemetry from transport-specific sources and
#   converts it into normalized gateway records.
# - Main data or concerns: Raw transport messages, decoded telemetry payloads, and
#   connection events.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.
# - Why this matters: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.

"""BLE ingestion adapter for physical pods.

This module is the gateway-side counterpart to the pod firmware BLE service.
Its job is not to store or forecast anything directly. Instead it discovers
pods, maintains one session per device, converts incoming samples into the
gateway's shared ``TelemetryRecord`` format, and forwards them into the
multi-source routing queue used by the rest of the gateway pipeline.
"""

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
# Function purpose: Implements the pod identifier from name step used by this
#   subsystem.
# - Project role: Belongs to the gateway ingestion layer and contributes one focused
#   step within that subsystem.
# - Inputs: Arguments such as name, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.

def _pod_id_from_name(name: str) -> str:
    parts = str(name).strip().split("-")
    candidate = parts[-1] if parts else name
    return candidate.zfill(2) if candidate.isdigit() else candidate
# Class purpose: Configuration for the multi-pod BLE ingestion wrapper.
# - Project role: Belongs to the gateway ingestion layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.

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
# Class purpose: Manage one or more live BLE pod sessions.
# - Project role: Belongs to the gateway ingestion layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.

class BleIngester:
    """Manage one or more live BLE pod sessions.

    In architectural terms this class is the ingestion bridge between the
    physical pod network and the gateway's unified internal data model. Both
    BLE and synthetic TCP sources are normalised into the same record shape so
    downstream validation, storage, forecasting, and dashboard logic can remain
    source-agnostic.
    """
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: Arguments such as queue, router, settings, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

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
    # Method purpose: Discover matching pods and launch one async session per
    #   target.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    async def start(self) -> None:
        """Discover matching pods and launch one async session per target."""
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
    # Method purpose: Implements the stop step used by this subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

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
    # Method purpose: Periodically refresh RSSI so link-quality history stays
    #   informative.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    async def refresh_rssi_loop(self) -> None:
        """Periodically refresh RSSI so link-quality history stays informative.

        RSSI is not used directly by the forecaster, but it matters for system
        diagnostics. The dashboard health and review pages can only explain poor
        data continuity if the gateway also tracks connection quality over time.
        """
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
    # Method purpose: Implements the make sample handler step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _make_sample_handler(self):
        # Method purpose: Implements the handle sample step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway ingestion layer and acts as
        #   a method on BleIngester.
        # - Inputs: Arguments such as record, _quality_flags, stats,
        #   timestamp, interpreted according to the rules encoded in the
        #   body below.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: All later persistence and forecasting logic
        #   depends on ingestion normalizing the live inputs correctly.
        # - Related flow: Receives BLE or TCP input and passes decoded
        #   records into routing and storage.

        async def _handle_sample(record, _quality_flags, stats, timestamp: str) -> None:
            # The BLE session already decoded the wire payload. At this point we
            # standardise the sample into the shared multi-source record format
            # used by the router and storage pipeline.
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
    # Method purpose: Implements the make corrupt handler step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _make_corrupt_handler(self, pod_id: str):
        # Method purpose: Implements the handle corrupt step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway ingestion layer and acts as
        #   a method on BleIngester.
        # - Inputs: No explicit arguments beyond module or instance context.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: All later persistence and forecasting logic
        #   depends on ingestion normalizing the live inputs correctly.
        # - Related flow: Receives BLE or TCP input and passes decoded
        #   records into routing and storage.

        async def _handle_corrupt() -> None:
            await self.router.note_corrupt(pod_id, "BLE")

        return _handle_corrupt
    # Method purpose: Implements the make connect handler step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _make_connect_handler(self, pod_id: str):
        # Method purpose: Implements the handle connect step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway ingestion layer and acts as
        #   a method on BleIngester.
        # - Inputs: Arguments such as is_reconnect, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: All later persistence and forecasting logic
        #   depends on ingestion normalizing the live inputs correctly.
        # - Related flow: Receives BLE or TCP input and passes decoded
        #   records into routing and storage.

        async def _handle_connect(is_reconnect: bool) -> None:
            self.router.note_connected(pod_id, "BLE")
            if is_reconnect:
                self.router.note_reconnect(pod_id, "BLE")

        return _handle_connect
    # Method purpose: Implements the make disconnect handler step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _make_disconnect_handler(self, pod_id: str):
        # Method purpose: Implements the handle disconnect step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway ingestion layer and acts as
        #   a method on BleIngester.
        # - Inputs: No explicit arguments beyond module or instance context.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: All later persistence and forecasting logic
        #   depends on ingestion normalizing the live inputs correctly.
        # - Related flow: Receives BLE or TCP input and passes decoded
        #   records into routing and storage.

        async def _handle_disconnect() -> None:
            self.router.note_disconnected(pod_id, "BLE")

        return _handle_disconnect
    # Method purpose: Resolve the set of pods that should be connected in this
    #   run.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns list[PodTarget] when the function completes
    #   successfully.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    async def _resolve_targets(self) -> list[PodTarget]:
        """Resolve the set of pods that should be connected in this run."""
        matches = await discover_matches(
            timeout=self.gateway_settings.scan_timeout_s,
            name_prefix=self.gateway_settings.device_name_scan_prefix,
            service_uuid=self.profile.service_uuid,
            addresses=self.gateway_settings.addresses,
        )
        return [PodTarget(address=match.address, name=match.name) for match in matches]
    # Method purpose: Implements the log task failure step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on BleIngester.
    # - Inputs: Arguments such as task, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

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
