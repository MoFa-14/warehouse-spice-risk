"""Built-in synthetic warehouse microclimate zone profiles."""

from __future__ import annotations

from dataclasses import dataclass


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


ZONE_PROFILES: dict[str, ZoneProfile] = {
    "interior_stable": ZoneProfile(
        name="interior_stable",
        label="ZONE_A",
        description=(
            "Interior aisle zone with low variance, modest drift, and infrequent disturbances. "
            "Represents a comparatively sheltered interior location."
        ),
        base_temp_c=20.8,
        base_rh_pct=38.0,
        noise_temp_c=0.15,
        noise_rh_pct=0.75,
        drift_temp_step_c=0.012,
        drift_rh_step_pct=0.05,
        drift_temp_limit_c=0.8,
        drift_rh_limit_pct=2.5,
        event_rate_per_hour=0.10,
        event_rate_active_hours_per_hour=0.35,
        event_spike_temp_c=0.7,
        event_spike_rh_pct=2.5,
        recovery_tau_seconds=1800.0,
    ),
    "entrance_disturbed": ZoneProfile(
        name="entrance_disturbed",
        label="ZONE_B",
        description=(
            "Entrance-facing zone with stronger fluctuations, higher humidity, and frequent door-opening "
            "style disturbance spikes during active hours."
        ),
        base_temp_c=24.2,
        base_rh_pct=49.0,
        noise_temp_c=0.45,
        noise_rh_pct=2.20,
        drift_temp_step_c=0.030,
        drift_rh_step_pct=0.12,
        drift_temp_limit_c=1.5,
        drift_rh_limit_pct=6.0,
        event_rate_per_hour=0.35,
        event_rate_active_hours_per_hour=1.40,
        event_spike_temp_c=1.9,
        event_spike_rh_pct=9.0,
        recovery_tau_seconds=900.0,
    ),
    "upper_rack_stratified": ZoneProfile(
        name="upper_rack_stratified",
        label="ZONE_C",
        description=(
            "Upper rack zone with a warmer, slightly drier baseline to emulate vertical stratification, "
            "plus moderate variance and disturbance sensitivity."
        ),
        base_temp_c=23.6,
        base_rh_pct=34.5,
        noise_temp_c=0.28,
        noise_rh_pct=1.20,
        drift_temp_step_c=0.020,
        drift_rh_step_pct=0.08,
        drift_temp_limit_c=1.1,
        drift_rh_limit_pct=4.0,
        event_rate_per_hour=0.18,
        event_rate_active_hours_per_hour=0.60,
        event_spike_temp_c=1.1,
        event_spike_rh_pct=4.0,
        recovery_tau_seconds=1400.0,
    ),
}


def zone_profile_names() -> tuple[str, ...]:
    """Return the stable CLI names for all built-in profiles."""
    return tuple(ZONE_PROFILES.keys())


def get_zone_profile(name: str) -> ZoneProfile:
    """Resolve one built-in zone profile by its CLI name."""
    try:
        return ZONE_PROFILES[str(name).strip().lower()]
    except KeyError as exc:
        valid = ", ".join(zone_profile_names())
        raise ValueError(f"Unknown zone profile {name!r}. Expected one of: {valid}.") from exc
