# File overview:
# - Responsibility: TCP listener for synthetic pods that speak newline-delimited
#   JSON.
# - Project role: Accepts live telemetry from transport-specific sources and
#   converts it into normalized gateway records.
# - Main data or concerns: Raw transport messages, decoded telemetry payloads, and
#   connection events.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.
# - Why this matters: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.

"""TCP listener for synthetic pods that speak newline-delimited JSON.

The synthetic pod cluster does not emulate BLE directly. Instead it sends the
same logical telemetry fields over a simpler TCP transport so the project can
demonstrate multi-pod scaling without needing many hardware units. This module
is therefore the synthetic-source ingestion boundary on the gateway side.
"""

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
# Class purpose: Configuration for synthetic pod TCP ingestion.
# - Project role: Belongs to the gateway ingestion layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.

@dataclass(frozen=True)
class TcpIngesterSettings:
    """Configuration for synthetic pod TCP ingestion."""

    host: str = "127.0.0.1"
    port: int = 8765
# Class purpose: Accept synthetic pod streams and normalise them for the gateway.
# - Project role: Belongs to the gateway ingestion layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: All later persistence and forecasting logic depends on
#   ingestion normalizing the live inputs correctly.
# - Related flow: Receives BLE or TCP input and passes decoded records into routing
#   and storage.

class TcpIngester:
    """Accept synthetic pod streams and normalise them for the gateway.

    The class mirrors the role of ``BleIngester`` for the software-generated
    pods. It decodes NDJSON telemetry lines, tracks reconnects, registers resend
    controllers, and pushes the resulting records into the same queue consumed
    by the rest of the gateway.
    """
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
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
    # Method purpose: Start the local TCP listener used by the synthetic pod
    #   cluster.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    async def start(self) -> None:
        """Start the local TCP listener used by the synthetic pod cluster."""
        self._server = await asyncio.start_server(self._accept_client, host=self.settings.host, port=self.settings.port)
        sockets = self._server.sockets or []
        for socket in sockets:
            LOGGER.debug("Synthetic TCP listener ready on %s", socket.getsockname())
    # Method purpose: Implements the stop step used by this subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

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
    # Method purpose: Implements the accept client step used by this subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: Arguments such as reader, writer, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _accept_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._handle_client(reader, writer), name="tcp-synthetic-client")
        self._client_tasks.add(task)
        # Method purpose: Implements the discard step used by this
        #   subsystem.
        # - Project role: Belongs to the gateway ingestion layer and acts as
        #   a method on TcpIngester.
        # - Inputs: Arguments such as completed, interpreted according to
        #   the rules encoded in the body below.
        # - Outputs: No direct return value; the function performs state
        #   updates or side effects.
        # - Important decisions: All later persistence and forecasting logic
        #   depends on ingestion normalizing the live inputs correctly.
        # - Related flow: Receives BLE or TCP input and passes decoded
        #   records into routing and storage.

        def _discard(completed: asyncio.Task[None]) -> None:
            self._client_tasks.discard(completed)

        task.add_done_callback(_discard)
    # Method purpose: Consume one synthetic pod connection until disconnect.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: Arguments such as reader, writer, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Consume one synthetic pod connection until disconnect.

        Each connection may yield many sequential telemetry rows. The handler is
        deliberately tolerant of corrupt lines because the purpose of the
        synthetic cluster is partly to exercise gateway robustness under less
        than perfect communication conditions.
        """
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
                    # Once queued, this record joins the exact same downstream
                    # validation and storage path used by BLE-origin samples.
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
    # Method purpose: Implements the note connection step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: All later persistence and forecasting logic depends
    #   on ingestion normalizing the live inputs correctly.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _note_connection(self, pod_id: str) -> None:
        count = self._connect_counts.get(pod_id, 0) + 1
        self._connect_counts[pod_id] = count
        if count > 1:
            self.router.note_reconnect(pod_id, "TCP")
    # Method purpose: Optionally append a latency-evaluation marker for offline
    #   analysis.
    # - Project role: Belongs to the gateway ingestion layer and acts as a
    #   method on TcpIngester.
    # - Inputs: Arguments such as pod_id, seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence-facing code centralizes storage rules
    #   so other modules do not duplicate schema or serialization assumptions.
    # - Related flow: Receives BLE or TCP input and passes decoded records into
    #   routing and storage.

    def _append_timing_event(self, *, pod_id: str, seq: int) -> None:
        """Optionally append a latency-evaluation marker for offline analysis."""
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
