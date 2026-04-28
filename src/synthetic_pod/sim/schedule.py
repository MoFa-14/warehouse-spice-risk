# File overview:
# - Responsibility: Operational-hours schedule helpers for synthetic warehouse
#   variability.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Operational-hours schedule helpers for synthetic warehouse variability."""

from __future__ import annotations

from dataclasses import dataclass


SECONDS_PER_HOUR = 3600.0
SECONDS_PER_DAY = 24.0 * SECONDS_PER_HOUR
# Class purpose: Simple uptime-modulo-24h schedule used by the synthetic pod.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass(frozen=True)
class ActiveHoursSchedule:
    """Simple uptime-modulo-24h schedule used by the synthetic pod."""

    active_start_hour: int = 8
    active_end_hour: int = 18
    active_noise_multiplier: float = 1.20
    inactive_noise_multiplier: float = 0.75
    # Method purpose: Return whether the synthetic warehouse is in its
    #   active-hours period.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ActiveHoursSchedule.
    # - Inputs: Arguments such as uptime_s, start_hour_offset, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

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
    # Method purpose: Return the local warehouse hour represented by the uptime
    #   and anchor offset.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ActiveHoursSchedule.
    # - Inputs: Arguments such as uptime_s, start_hour_offset, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def hour_of_day(self, uptime_s: float, start_hour_offset: float = 0.0) -> float:
        """Return the local warehouse hour represented by the uptime and anchor offset."""
        return (((float(uptime_s) % SECONDS_PER_DAY) / SECONDS_PER_HOUR) + float(start_hour_offset)) % 24.0
    # Method purpose: Resolve the disturbance event rate for the current
    #   schedule period.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ActiveHoursSchedule.
    # - Inputs: Arguments such as base_rate_per_hour, active_rate_per_hour,
    #   uptime_s, start_hour_offset, interpreted according to the rules encoded
    #   in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

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
    # Method purpose: Return the schedule-dependent noise multiplier.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on ActiveHoursSchedule.
    # - Inputs: Arguments such as uptime_s, start_hour_offset, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def noise_multiplier(self, uptime_s: float, start_hour_offset: float = 0.0) -> float:
        """Return the schedule-dependent noise multiplier."""
        if self.is_active(uptime_s, start_hour_offset):
            return float(self.active_noise_multiplier)
        return float(self.inactive_noise_multiplier)
