# File overview:
# - Responsibility: Resend request abstractions shared by BLE and TCP pod sources.
# - Project role: Handles resend requests and other pod-control actions.
# - Main data or concerns: Sequence ranges, control payloads, and pod command
#   values.
# - Related flow: Bridges operator or runtime control requests to lower transport
#   actions.
# - Why this matters: Control behavior must stay explicit because it changes what
#   the pod sends next.

"""Resend request abstractions shared by BLE and TCP pod sources."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Protocol

from gateway.protocol.ndjson import encode_ndjson_line


LOGGER = logging.getLogger(__name__)
# Class purpose: Control channel that can request one or more samples to be
#   replayed.
# - Project role: Belongs to the gateway control layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Control behavior must stay explicit because it changes what
#   the pod sends next.
# - Related flow: Bridges operator or runtime control requests to lower transport
#   actions.

class ResendController(Protocol):
    """Control channel that can request one or more samples to be replayed."""
    # Method purpose: Implements the request seq step used by this subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on ResendController.
    # - Inputs: Arguments such as pod_id, seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_seq(self, pod_id: str, seq: int) -> None:
        ...
    # Method purpose: Implements the request from seq step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on ResendController.
    # - Inputs: Arguments such as pod_id, from_seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        ...
# Class purpose: Best-effort resend requests over the pod BLE control
#   characteristic.
# - Project role: Belongs to the gateway control layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Control behavior must stay explicit because it changes what
#   the pod sends next.
# - Related flow: Bridges operator or runtime control requests to lower transport
#   actions.

@dataclass
class BleResendController:
    """Best-effort resend requests over the pod BLE control characteristic."""

    session: object
    # Method purpose: Implements the request seq step used by this subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on BleResendController.
    # - Inputs: Arguments such as pod_id, seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_seq(self, pod_id: str, seq: int) -> None:
        LOGGER.debug("REQ_SEQ pod=%s seq=%s (BLE stub)", pod_id, seq)
        await self.session.request_resend_seq(int(seq))
    # Method purpose: Implements the request from seq step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on BleResendController.
    # - Inputs: Arguments such as pod_id, from_seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        LOGGER.debug("REQ_FROM_SEQ pod=%s from_seq=%s (BLE stub)", pod_id, from_seq)
        await self.session.request_resend_from_seq(int(from_seq))
# Class purpose: Real resend requests sent back to the synthetic pod over TCP.
# - Project role: Belongs to the gateway control layer and groups related state or
#   behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Control behavior must stay explicit because it changes what
#   the pod sends next.
# - Related flow: Bridges operator or runtime control requests to lower transport
#   actions.

@dataclass
class TcpResendController:
    """Real resend requests sent back to the synthetic pod over TCP."""

    writer: asyncio.StreamWriter
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Method purpose: Implements the request seq step used by this subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on TcpResendController.
    # - Inputs: Arguments such as pod_id, seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_seq(self, pod_id: str, seq: int) -> None:
        LOGGER.debug("REQ_SEQ pod=%s seq=%s", pod_id, seq)
        await self._send({"cmd": "REQ_SEQ", "pod_id": pod_id, "seq": int(seq)})
    # Method purpose: Implements the request from seq step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on TcpResendController.
    # - Inputs: Arguments such as pod_id, from_seq, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def request_from_seq(self, pod_id: str, from_seq: int) -> None:
        LOGGER.debug("REQ_FROM_SEQ pod=%s from_seq=%s", pod_id, from_seq)
        await self._send({"cmd": "REQ_FROM_SEQ", "pod_id": pod_id, "from_seq": int(from_seq)})
    # Method purpose: Implements the send step used by this subsystem.
    # - Project role: Belongs to the gateway control layer and acts as a method
    #   on TcpResendController.
    # - Inputs: Arguments such as payload, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Control behavior must stay explicit because it
    #   changes what the pod sends next.
    # - Related flow: Bridges operator or runtime control requests to lower
    #   transport actions.

    async def _send(self, payload: dict[str, object]) -> None:
        if self.writer.is_closing():
            LOGGER.warning("Cannot send resend request because the TCP writer is already closing: %s", json.dumps(payload))
            return
        async with self._lock:
            self.writer.write(encode_ndjson_line(payload))
            await self.writer.drain()
