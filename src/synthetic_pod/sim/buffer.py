"""Replay buffer for resend requests from the gateway."""

from __future__ import annotations

from collections import deque
from typing import Iterable


class ReplayBuffer:
    """Keep a rolling window of generated samples by sequence number."""

    def __init__(self, maxlen: int = 300) -> None:
        self.maxlen = maxlen
        self._items: deque[dict[str, object]] = deque(maxlen=maxlen)

    def add(self, sample: dict[str, object]) -> None:
        self._items.append(dict(sample))

    def get(self, seq: int) -> dict[str, object] | None:
        for item in reversed(self._items):
            if int(item["seq"]) == int(seq):
                return dict(item)
        return None

    def iter_from_seq(self, from_seq: int) -> Iterable[dict[str, object]]:
        for item in self._items:
            if int(item["seq"]) >= int(from_seq):
                yield dict(item)
