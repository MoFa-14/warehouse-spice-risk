# File overview:
# - Responsibility: Replay buffer for resend requests from the gateway.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Replay buffer for resend requests from the gateway."""

from __future__ import annotations

from collections import deque
from typing import Iterable
# Class purpose: Keep a rolling window of generated samples by sequence number.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

class ReplayBuffer:
    """Keep a rolling window of generated samples by sequence number."""
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ReplayBuffer.
    # - Inputs: Arguments such as maxlen, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def __init__(self, maxlen: int = 300) -> None:
        self.maxlen = maxlen
        self._items: deque[dict[str, object]] = deque(maxlen=maxlen)
    # Method purpose: Implements the add step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ReplayBuffer.
    # - Inputs: Arguments such as sample, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def add(self, sample: dict[str, object]) -> None:
        self._items.append(dict(sample))
    # Method purpose: Implements the get step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ReplayBuffer.
    # - Inputs: Arguments such as seq, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns dict[str, object] | None when the function completes
    #   successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def get(self, seq: int) -> dict[str, object] | None:
        for item in reversed(self._items):
            if int(item["seq"]) == int(seq):
                return dict(item)
        return None
    # Method purpose: Implements the iter from seq step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ReplayBuffer.
    # - Inputs: Arguments such as from_seq, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns Iterable[dict[str, object]] when the function completes
    #   successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def iter_from_seq(self, from_seq: int) -> Iterable[dict[str, object]]:
        for item in self._items:
            if int(item["seq"]) >= int(from_seq):
                yield dict(item)
