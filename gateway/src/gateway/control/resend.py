"""Resend request abstractions shared by BLE and TCP pod sources."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Protocol

from gateway.protocol.ndjson import encode_ndjson_line


LOGGER = logging.getLogger(__name__)


class ResendController(Protocol):
    """Control channel that can request one or more samples to be replayed."""

    async def request_seq(self, pod_id: str, seq: int) -> None:
        ...

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        ...


@dataclass
class BleResendController:
    """Best-effort resend requests over the pod BLE control characteristic."""

    session: object

    async def request_seq(self, pod_id: str, seq: int) -> None:
        LOGGER.info("REQ_SEQ pod=%s seq=%s (BLE stub)", pod_id, seq)
        await self.session.request_resend_seq(int(seq))

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        LOGGER.info("REQ_FROM_SEQ pod=%s from_seq=%s (BLE stub)", pod_id, from_seq)
        await self.session.request_resend_from_seq(int(from_seq))


@dataclass
class TcpResendController:
    """Real resend requests sent back to the synthetic pod over TCP."""

    writer: asyncio.StreamWriter
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def request_seq(self, pod_id: str, seq: int) -> None:
        LOGGER.info("REQ_SEQ pod=%s seq=%s", pod_id, seq)
        await self._send({"cmd": "REQ_SEQ", "pod_id": pod_id, "seq": int(seq)})

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        LOGGER.info("REQ_FROM_SEQ pod=%s from_seq=%s", pod_id, from_seq)
        await self._send({"cmd": "REQ_FROM_SEQ", "pod_id": pod_id, "from_seq": int(from_seq)})

    async def _send(self, payload: dict[str, object]) -> None:
        if self.writer.is_closing():
            LOGGER.warning("Cannot send resend request because the TCP writer is already closing: %s", json.dumps(payload))
            return
        async with self._lock:
            self.writer.write(encode_ndjson_line(payload))
            await self.writer.drain()
