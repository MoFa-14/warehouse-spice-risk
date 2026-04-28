# File overview:
# - Responsibility: Fault injection for the synthetic pod, including optional burst
#   loss.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Fault injection for the synthetic pod, including optional burst loss."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
# Class purpose: Delivery behavior selected for one generated sample.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

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
# Class purpose: Probabilities and ranges used to stress the multi-pod gateway.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

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
    # Method purpose: Constrains probability to the safe range expected by later
    #   logic.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on FaultProfile.
    # - Inputs: Arguments such as value, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    @staticmethod
    def _clamp_probability(value: float) -> float:
        return max(0.0, min(1.0, float(value)))
    # Method purpose: Implements the scaled probability step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on FaultProfile.
    # - Inputs: Arguments such as probability, burst_active, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def scaled_probability(self, probability: float, *, burst_active: bool) -> float:
        scaled = float(probability) * (self.burst_multiplier if burst_active else 1.0)
        return self._clamp_probability(scaled)
# Class purpose: Stateful fault generator so loss and delay can occur in bursts.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass
class FaultController:
    """Stateful fault generator so loss and delay can occur in bursts."""

    profile: FaultProfile
    interval_s: float
    rng: random.Random = field(default_factory=random.Random)
    burst_remaining_s: float = 0.0
    # Method purpose: Implements the choose action step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on FaultController.
    # - Inputs: Arguments such as disturbance_active, interpreted according to
    #   the rules encoded in the body below.
    # - Outputs: Returns FaultAction when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

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
    # Method purpose: Implements the maybe activate burst step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on FaultController.
    # - Inputs: Arguments such as disturbance_active, interpreted according to
    #   the rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

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
    # Method purpose: Implements the advance burst window step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on FaultController.
    # - Inputs: Arguments such as burst_active, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _advance_burst_window(self, burst_active: bool) -> None:
        if not burst_active:
            return
        self.burst_remaining_s = max(0.0, self.burst_remaining_s - float(self.interval_s))
