"""Fault injection for the synthetic pod."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class FaultAction:
    """Delivery behavior selected for one generated sample."""

    drop: bool = False
    corrupt: bool = False
    delay_s: float = 0.0
    disconnect_s: float = 0.0


@dataclass(frozen=True)
class FaultProfile:
    """Probabilities and ranges used to stress the multi-pod gateway."""

    p_drop: float = 0.0
    p_corrupt: float = 0.0
    p_delay: float = 0.0
    p_disconnect: float = 0.0
    max_delay_s: float = 5.0
    disconnect_min_s: float = 2.0
    disconnect_max_s: float = 10.0

    def choose_action(self) -> FaultAction:
        disconnect_s = 0.0
        if random.random() < self.p_disconnect:
            disconnect_s = random.uniform(self.disconnect_min_s, self.disconnect_max_s)

        if disconnect_s > 0:
            return FaultAction(disconnect_s=disconnect_s)

        drop = random.random() < self.p_drop
        corrupt = random.random() < self.p_corrupt
        delay_s = random.uniform(0.0, self.max_delay_s) if random.random() < self.p_delay else 0.0
        return FaultAction(drop=drop, corrupt=corrupt, delay_s=delay_s)
