# File overview:
# - Responsibility: Simple exponential backoff helper for reconnect loops.
# - Project role: Provides reusable low-level helpers for timing, retry logic, and
#   sequence handling.
# - Main data or concerns: Helper arguments, timestamps, counters, and shared return
#   values.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.
# - Why this matters: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.

"""Simple exponential backoff helper for reconnect loops."""

from __future__ import annotations

from dataclasses import dataclass, field
# Class purpose: Bounded exponential backoff with reset support.
# - Project role: Belongs to the shared gateway utility layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.

@dataclass
class ExponentialBackoff:
    """Bounded exponential backoff with reset support."""

    initial_delay_s: float = 1.0
    factor: float = 2.0
    max_delay_s: float = 30.0
    _next_delay_s: float = field(default=0.0, init=False, repr=False)
    # Method purpose: Return the next delay and update the internal state.
    # - Project role: Belongs to the shared gateway utility layer and acts as a
    #   method on ExponentialBackoff.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Keeping small utility rules centralized prevents
    #   subtle duplication across transport and storage code.
    # - Related flow: Supports higher-level gateway modules with focused helper
    #   behavior.

    def next_delay(self) -> float:
        """Return the next delay and update the internal state."""
        if self._next_delay_s <= 0:
            self._next_delay_s = self.initial_delay_s
        else:
            self._next_delay_s = min(self._next_delay_s * self.factor, self.max_delay_s)
        return self._next_delay_s
    # Method purpose: Reset the backoff sequence.
    # - Project role: Belongs to the shared gateway utility layer and acts as a
    #   method on ExponentialBackoff.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Keeping small utility rules centralized prevents
    #   subtle duplication across transport and storage code.
    # - Related flow: Supports higher-level gateway modules with focused helper
    #   behavior.

    def reset(self) -> None:
        """Reset the backoff sequence."""
        self._next_delay_s = 0.0
