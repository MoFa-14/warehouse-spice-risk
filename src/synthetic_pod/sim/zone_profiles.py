# File overview:
# - Responsibility: Built-in synthetic warehouse microclimate zone profiles.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Built-in synthetic warehouse microclimate zone profiles."""

from __future__ import annotations

from dataclasses import dataclass
# Class purpose: Default parameters for one synthetic warehouse micro-zone.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass(frozen=True)
class ZoneProfile:
    """Default parameters for one synthetic warehouse micro-zone."""

    name: str
    label: str
    description: str
    base_temp_c: float
    base_rh_pct: float
    noise_temp_c: float
    noise_rh_pct: float
    drift_temp_step_c: float
    drift_rh_step_pct: float
    drift_temp_limit_c: float
    drift_rh_limit_pct: float
    event_rate_per_hour: float
    event_rate_active_hours_per_hour: float
    event_spike_temp_c: float
    event_spike_rh_pct: float
    recovery_tau_seconds: float
    baseline_reversion_tau_seconds: float
    seasonal_temp_weight: float
    seasonal_rh_weight: float
    diurnal_temp_weight: float
    diurnal_rh_weight: float


ZONE_PROFILES: dict[str, ZoneProfile] = {
    "interior_stable": ZoneProfile(
        name="interior_stable",
        label="ZONE_A",
        description=(
            "Interior aisle zone with low variance, modest drift, and infrequent disturbances. "
            "Represents a comparatively sheltered interior location."
        ),
        base_temp_c=18.9,
        base_rh_pct=46.0,
        noise_temp_c=0.10,
        noise_rh_pct=0.45,
        drift_temp_step_c=0.008,
        drift_rh_step_pct=0.03,
        drift_temp_limit_c=0.6,
        drift_rh_limit_pct=1.8,
        event_rate_per_hour=0.10,
        event_rate_active_hours_per_hour=0.25,
        event_spike_temp_c=0.45,
        event_spike_rh_pct=1.6,
        recovery_tau_seconds=2200.0,
        baseline_reversion_tau_seconds=18000.0,
        seasonal_temp_weight=0.28,
        seasonal_rh_weight=0.20,
        diurnal_temp_weight=0.18,
        diurnal_rh_weight=0.12,
    ),
    "entrance_disturbed": ZoneProfile(
        name="entrance_disturbed",
        label="ZONE_B",
        description=(
            "Entrance-facing zone with modest operational movement, slightly elevated humidity, and "
            "gentle door-opening style disturbances that still track indoor warehouse inertia."
        ),
        base_temp_c=18.4,
        base_rh_pct=52.0,
        noise_temp_c=0.16,
        noise_rh_pct=0.70,
        drift_temp_step_c=0.010,
        drift_rh_step_pct=0.04,
        drift_temp_limit_c=0.75,
        drift_rh_limit_pct=2.8,
        event_rate_per_hour=0.12,
        event_rate_active_hours_per_hour=0.35,
        event_spike_temp_c=0.65,
        event_spike_rh_pct=2.6,
        recovery_tau_seconds=2000.0,
        baseline_reversion_tau_seconds=14400.0,
        seasonal_temp_weight=0.42,
        seasonal_rh_weight=0.32,
        diurnal_temp_weight=0.26,
        diurnal_rh_weight=0.18,
    ),
    "upper_rack_stratified": ZoneProfile(
        name="upper_rack_stratified",
        label="ZONE_C",
        description=(
            "Upper rack zone with a warmer, slightly drier baseline to emulate vertical stratification, "
            "plus moderate variance and disturbance sensitivity."
        ),
        base_temp_c=20.1,
        base_rh_pct=43.0,
        noise_temp_c=0.14,
        noise_rh_pct=0.55,
        drift_temp_step_c=0.010,
        drift_rh_step_pct=0.04,
        drift_temp_limit_c=0.8,
        drift_rh_limit_pct=2.4,
        event_rate_per_hour=0.18,
        event_rate_active_hours_per_hour=0.45,
        event_spike_temp_c=0.55,
        event_spike_rh_pct=1.9,
        recovery_tau_seconds=1800.0,
        baseline_reversion_tau_seconds=15600.0,
        seasonal_temp_weight=0.34,
        seasonal_rh_weight=0.24,
        diurnal_temp_weight=0.22,
        diurnal_rh_weight=0.14,
    ),
}
# Function purpose: Return the stable CLI names for all built-in profiles.
# - Project role: Belongs to the synthetic pod simulation layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns tuple[str, ...] when the function completes successfully.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

def zone_profile_names() -> tuple[str, ...]:
    """Return the stable CLI names for all built-in profiles."""
    return tuple(ZONE_PROFILES.keys())
# Function purpose: Resolve one built-in zone profile by its CLI name.
# - Project role: Belongs to the synthetic pod simulation layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as name, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns ZoneProfile when the function completes successfully.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

def get_zone_profile(name: str) -> ZoneProfile:
    """Resolve one built-in zone profile by its CLI name."""
    try:
        return ZONE_PROFILES[str(name).strip().lower()]
    except KeyError as exc:
        valid = ", ".join(zone_profile_names())
        raise ValueError(f"Unknown zone profile {name!r}. Expected one of: {valid}.") from exc
