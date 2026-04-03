"""TCP listener for synthetic pods that speak NDJSON telemetry."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from gateway.control.resend import TcpResendController
from gateway.multi.record import TelemetryRecord
from gateway.multi.router import PodRouter
from gateway.protocol.decoder import DecodeError, decode_telemetry_payload
from gateway.protocol.ndjson import decode_ndjson_line
from gateway.utils.timeutils import utc_now_iso


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TcpIngesterSettings:
    """Configuration for synthetic pod TCP ingestion."""

    host: str = "127.0.0.1"
    port: int = 8765


class TcpIngester:
    """Accept synthetic pod connections and push normalized records into the queue."""

    def __init__(
        self,
        *,
        queue: asyncio.Queue[TelemetryRecord],
        router: PodRouter,
        settings: TcpIngesterSettings,
    ) -> None:
        self.queue = queue
        self.router = router
        self.settings = settings
        self._server: asyncio.AbstractServer | None = None
        self._stop_event = asyncio.Event()
        self._connect_counts: dict[str, int] = {}
        self._client_tasks: set[asyncio.Task[None]] = set()
        timing_path = str(os.getenv("DSP_EVAL_TIMING_LOG", "")).strip()
        self._timing_log_path = Path(timing_path).expanduser() if timing_path else None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept_client, host=self.settings.host, port=self.settings.port)
        sockets = self._server.sockets or []
        for socket in sockets:
            LOGGER.debug("Synthetic TCP listener ready on %s", socket.getsockname())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in list(self._client_tasks):
            task.cancel()
        if self._client_tasks:
            await asyncio.gather(*self._client_tasks, return_exceptions=True)
            self._client_tasks.clear()

    def _accept_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._handle_client(reader, writer), name="tcp-synthetic-client")
        self._client_tasks.add(task)

        def _discard(completed: asyncio.Task[None]) -> None:
            self._client_tasks.discard(completed)

        task.add_done_callback(_discard)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        LOGGER.debug("Synthetic pod connected from %s", peer)
        controller = TcpResendController(writer)
        known_pod_id: str | None = None
        connection_registered = False

        try:
            while not self._stop_event.is_set():
                line = await reader.readline()
                if not line:
                    break
                if not line.strip():
                    continue

                try:
                    payload = decode_ndjson_line(line)
                    if "cmd" in payload:
                        LOGGER.debug("Ignoring unexpected command payload from synthetic pod: %s", payload)
                        continue
                    decoded = decode_telemetry_payload(payload)
                except (DecodeError, ValueError) as exc:
                    LOGGER.debug("Discarding corrupt TCP telemetry line from %s: %s", peer, exc)
                    if known_pod_id is not None:
                        await self.router.note_corrupt(known_pod_id, "TCP")
                    continue

                known_pod_id = decoded.pod_id
                self.router.register_resend_controller(known_pod_id, controller)
                if not connection_registered:
                    self._note_connection(known_pod_id)
                    self.router.note_connected(known_pod_id, "TCP")
                    connection_registered = True

                await self.queue.put(
                    TelemetryRecord(
                        pod_id=decoded.pod_id,
                        seq=decoded.seq,
                        ts_uptime_s=decoded.ts_uptime_s,
                        temp_c=decoded.temp_c,
                        rh_pct=decoded.rh_pct,
                        flags=decoded.flags,
                        rssi=None,
                        source="TCP",
                        ts_pc_utc=utc_now_iso(),
                    )
                )
                self._append_timing_event(pod_id=decoded.pod_id, seq=decoded.seq)
        finally:
            if known_pod_id is not None:
                self.router.note_disconnected(known_pod_id, "TCP")
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            LOGGER.debug("Synthetic pod disconnected from %s", peer)

    def _note_connection(self, pod_id: str) -> None:
        count = self._connect_counts.get(pod_id, 0) + 1
        self._connect_counts[pod_id] = count
        if count > 1:
            self.router.note_reconnect(pod_id, "TCP")

    def _append_timing_event(self, *, pod_id: str, seq: int) -> None:
        if self._timing_log_path is None:
            return
        payload = {
            "event": "gateway_accepted",
            "pod_id": str(pod_id),
            "seq": int(seq),
            "source": "TCP",
            "ts_gateway_accepted_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
        self._timing_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._timing_log_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True))
            handle.write("\n")
