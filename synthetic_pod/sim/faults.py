"""Fault injection for the synthetic pod, including optional burst loss."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FaultAction:
    """Delivery behavior selected for one generated sample."""

    drop: bool = False
    corrupt: bool = False
    delay_s: float = 0.0
    disconnect_s: float = 0.0
    burst_active: bool = False
    effective_p_drop: float = 0.0
    effective_p_delay: float = 0.0


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
    burst_loss_enabled: bool = False
    burst_duration_s: float = 30.0
    burst_multiplier: float = 3.0
    burst_trigger_probability: float = 0.40

    @staticmethod
    def _clamp_probability(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def scaled_probability(self, probability: float, *, burst_active: bool) -> float:
        scaled = float(probability) * (self.burst_multiplier if burst_active else 1.0)
        return self._clamp_probability(scaled)


@dataclass
class FaultController:
    """Stateful fault generator so loss and delay can occur in bursts."""

    profile: FaultProfile
    interval_s: float
    rng: random.Random = field(default_factory=random.Random)
    burst_remaining_s: float = 0.0

    def choose_action(self, *, disturbance_active: bool) -> FaultAction:
        burst_active = self._maybe_activate_burst(disturbance_active)

        if self.rng.random() < self.profile.p_disconnect:
            disconnect_s = self.rng.uniform(self.profile.disconnect_min_s, self.profile.disconnect_max_s)
            self._advance_burst_window(burst_active)
            return FaultAction(
                disconnect_s=disconnect_s,
                burst_active=burst_active,
                effective_p_drop=self.profile.scaled_probability(self.profile.p_drop, burst_active=burst_active),
                effective_p_delay=self.profile.scaled_probability(self.profile.p_delay, burst_active=burst_active),
            )

        effective_p_drop = self.profile.scaled_probability(self.profile.p_drop, burst_active=burst_active)
        effective_p_delay = self.profile.scaled_probability(self.profile.p_delay, burst_active=burst_active)
        drop = self.rng.random() < effective_p_drop
        corrupt = self.rng.random() < self.profile.p_corrupt
        delay_s = self.rng.uniform(0.0, self.profile.max_delay_s) if self.rng.random() < effective_p_delay else 0.0

        self._advance_burst_window(burst_active)
        return FaultAction(
            drop=drop,
            corrupt=corrupt,
            delay_s=delay_s,
            burst_active=burst_active,
            effective_p_drop=effective_p_drop,
            effective_p_delay=effective_p_delay,
        )

    def _maybe_activate_burst(self, disturbance_active: bool) -> bool:
        burst_active = self.burst_remaining_s > 0.0
        if burst_active:
            return True
        if not self.profile.burst_loss_enabled or not disturbance_active:
            return False
        if self.rng.random() < self.profile.burst_trigger_probability:
            self.burst_remaining_s = max(float(self.profile.burst_duration_s), float(self.interval_s))
            return True
        return False

    def _advance_burst_window(self, burst_active: bool) -> None:
        if not burst_active:
            return
        self.burst_remaining_s = max(0.0, self.burst_remaining_s - float(self.interval_s))
