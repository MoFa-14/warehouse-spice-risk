"""Simple exponential backoff helper for reconnect loops."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ExponentialBackoff:
    """Bounded exponential backoff with reset support."""

    initial_delay_s: float = 1.0
    factor: float = 2.0
    max_delay_s: float = 30.0
    _next_delay_s: float = field(default=0.0, init=False, repr=False)

    def next_delay(self) -> float:
        """Return the next delay and update the internal state."""
        if self._next_delay_s <= 0:
            self._next_delay_s = self.initial_delay_s
        else:
            self._next_delay_s = min(self._next_delay_s * self.factor, self.max_delay_s)
        return self._next_delay_s

    def reset(self) -> None:
        """Reset the backoff sequence."""
        self._next_delay_s = 0.0
