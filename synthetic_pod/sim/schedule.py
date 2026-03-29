"""Operational-hours schedule helpers for synthetic warehouse variability."""

from __future__ import annotations

from dataclasses import dataclass


SECONDS_PER_HOUR = 3600.0
SECONDS_PER_DAY = 24.0 * SECONDS_PER_HOUR


@dataclass(frozen=True)
class ActiveHoursSchedule:
    """Simple uptime-modulo-24h schedule used by the synthetic pod."""

    active_start_hour: int = 8
    active_end_hour: int = 18
    active_noise_multiplier: float = 1.20
    inactive_noise_multiplier: float = 0.75

    def is_active(self, uptime_s: float, start_hour_offset: float = 0.0) -> bool:
        """Return whether the synthetic warehouse is in its active-hours period."""
        hour = self.hour_of_day(uptime_s, start_hour_offset)
        start = self.active_start_hour % 24
        end = self.active_end_hour % 24
        if start == end:
            return True
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def hour_of_day(self, uptime_s: float, start_hour_offset: float = 0.0) -> float:
        """Return the local warehouse hour represented by the uptime and anchor offset."""
        return (((float(uptime_s) % SECONDS_PER_DAY) / SECONDS_PER_HOUR) + float(start_hour_offset)) % 24.0

    def event_rate_per_hour(
        self,
        *,
        base_rate_per_hour: float,
        active_rate_per_hour: float | None,
        uptime_s: float,
        start_hour_offset: float = 0.0,
    ) -> float:
        """Resolve the disturbance event rate for the current schedule period."""
        if self.is_active(uptime_s, start_hour_offset) and active_rate_per_hour is not None:
            return max(0.0, float(active_rate_per_hour))
        return max(0.0, float(base_rate_per_hour))

    def noise_multiplier(self, uptime_s: float, start_hour_offset: float = 0.0) -> float:
        """Return the schedule-dependent noise multiplier."""
        if self.is_active(uptime_s, start_hour_offset):
            return float(self.active_noise_multiplier)
        return float(self.inactive_noise_multiplier)
