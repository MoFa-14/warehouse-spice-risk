# File overview:
# - Responsibility: Synthetic pod helpers.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Synthetic pod helpers."""

from .buffer import ReplayBuffer
from .faults import FaultAction, FaultController, FaultProfile
from .generator import GeneratedTelemetrySample, MicroclimateConfig, SyntheticTelemetryGenerator
from .schedule import ActiveHoursSchedule
from .zone_profiles import ZoneProfile, get_zone_profile, zone_profile_names

__all__ = [
    "FaultAction",
    "FaultController",
    "FaultProfile",
    "GeneratedTelemetrySample",
    "ActiveHoursSchedule",
    "MicroclimateConfig",
    "ReplayBuffer",
    "SyntheticTelemetryGenerator",
    "ZoneProfile",
    "get_zone_profile",
    "zone_profile_names",
]
