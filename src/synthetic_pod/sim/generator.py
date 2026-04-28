# File overview:
# - Responsibility: Synthetic warehouse microclimate model for pod 02.
# - Project role: Generates simulated pod behavior, schedules, faults, and
#   environmental patterns.
# - Main data or concerns: Synthetic sensor values, schedules, weather trends, and
#   simulated disturbances.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.
# - Why this matters: Simulation modules matter because they extend the single
#   physical pod into a multi-zone experimental system.

"""Synthetic warehouse microclimate model for pod 02."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sim.schedule import ActiveHoursSchedule
from sim.weather import IndoorClimateTarget, bristol_indoor_target
from sim.zone_profiles import ZoneProfile
# Class purpose: Resolved generation parameters for one synthetic zone.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass(frozen=True)
class MicroclimateConfig:
    """Resolved generation parameters for one synthetic zone."""

    pod_id: str
    interval_s: int
    zone_profile_name: str
    base_temp_c: float
    base_rh_pct: float
    noise_temp_c: float
    noise_rh_pct: float
    drift_temp_step_c: float
    drift_rh_step_pct: float
    drift_temp_limit_c: float
    drift_rh_limit_pct: float
    event_rate_per_hour: float
    event_rate_active_hours_per_hour: float | None
    event_spike_temp_c: float
    event_spike_rh_pct: float
    recovery_tau_seconds: float
    baseline_reversion_tau_seconds: float
    seasonal_temp_weight: float
    seasonal_rh_weight: float
    diurnal_temp_weight: float
    diurnal_rh_weight: float
    timezone_name: str
    start_local_time: datetime
    temp_min_c: float = -5.0
    temp_max_c: float = 45.0
    rh_min_pct: float = 0.0
    rh_max_pct: float = 100.0
    flags: int = 0
    disturbance_temp_threshold_c: float = 0.10
    disturbance_rh_threshold_pct: float = 0.35
# Class purpose: One generated telemetry sample plus modeling context for logging.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass(frozen=True)
class GeneratedTelemetrySample:
    """One generated telemetry sample plus modeling context for logging."""

    pod_id: str
    seq: int
    ts_uptime_s: float
    temp_c: float
    rh_pct: float
    flags: int
    zone_profile: str
    disturbance_active: bool
    disturbance_just_triggered: bool
    active_hours: bool
    baseline_temp_c: float
    baseline_rh_pct: float
    # Method purpose: Return the wire payload consumed by the gateway.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on GeneratedTelemetrySample.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns dict[str, object] when the function completes
    #   successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def to_payload(self) -> dict[str, object]:
        """Return the wire payload consumed by the gateway."""
        return {
            "pod_id": self.pod_id,
            "seq": self.seq,
            "ts_uptime_s": round(self.ts_uptime_s, 1),
            "temp_c": round(self.temp_c, 3),
            "rh_pct": round(self.rh_pct, 3),
            "flags": self.flags,
        }
# Class purpose: Generate time-varying warehouse microclimates with disturbances and
#   recovery.
# - Project role: Belongs to the synthetic pod simulation layer and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

@dataclass
class SyntheticTelemetryGenerator:
    """Generate time-varying warehouse microclimates with disturbances and recovery."""

    config: MicroclimateConfig
    schedule: ActiveHoursSchedule = field(default_factory=ActiveHoursSchedule)
    rng: random.Random = field(default_factory=random.Random)
    seq: int = 0
    uptime_s: float = 0.0
    temp_drift_c: float = 0.0
    rh_drift_pct: float = 0.0
    disturbance_temp_c: float = 0.0
    disturbance_rh_pct: float = 0.0
    # Method purpose: Implements the from zone profile step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as pod_id, interval_s, zone_profile, base_temp_c,
    #   base_rh_pct, noise_temp_c, noise_rh_pct, drift_temp_step_c,
    #   drift_rh_step_pct, event_rate_per_hour,
    #   event_rate_active_hours_per_hour, event_spike_temp_c,
    #   event_spike_rh_pct, recovery_tau_seconds,
    #   baseline_reversion_tau_seconds, seasonal_temp_weight,
    #   seasonal_rh_weight, diurnal_temp_weight, diurnal_rh_weight,
    #   timezone_name, start_local_time, schedule, rng, interpreted according to
    #   the rules encoded in the body below.
    # - Outputs: Returns 'SyntheticTelemetryGenerator' when the function
    #   completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    @classmethod
    def from_zone_profile(
        cls,
        *,
        pod_id: str,
        interval_s: int,
        zone_profile: ZoneProfile,
        base_temp_c: float | None = None,
        base_rh_pct: float | None = None,
        noise_temp_c: float | None = None,
        noise_rh_pct: float | None = None,
        drift_temp_step_c: float | None = None,
        drift_rh_step_pct: float | None = None,
        event_rate_per_hour: float | None = None,
        event_rate_active_hours_per_hour: float | None = None,
        event_spike_temp_c: float | None = None,
        event_spike_rh_pct: float | None = None,
        recovery_tau_seconds: float | None = None,
        baseline_reversion_tau_seconds: float | None = None,
        seasonal_temp_weight: float | None = None,
        seasonal_rh_weight: float | None = None,
        diurnal_temp_weight: float | None = None,
        diurnal_rh_weight: float | None = None,
        timezone_name: str = "Europe/London",
        start_local_time: datetime | None = None,
        schedule: ActiveHoursSchedule | None = None,
        rng: random.Random | None = None,
    ) -> "SyntheticTelemetryGenerator":
        zoneinfo = _resolve_zoneinfo(timezone_name)
        resolved_start_local_time = start_local_time or _default_start_local_time(zoneinfo)
        if zoneinfo is not None:
            if resolved_start_local_time.tzinfo is None:
                resolved_start_local_time = resolved_start_local_time.replace(tzinfo=zoneinfo)
            else:
                resolved_start_local_time = resolved_start_local_time.astimezone(zoneinfo)
        config = MicroclimateConfig(
            pod_id=pod_id,
            interval_s=int(interval_s),
            zone_profile_name=zone_profile.name,
            base_temp_c=zone_profile.base_temp_c if base_temp_c is None else float(base_temp_c),
            base_rh_pct=zone_profile.base_rh_pct if base_rh_pct is None else float(base_rh_pct),
            noise_temp_c=zone_profile.noise_temp_c if noise_temp_c is None else float(noise_temp_c),
            noise_rh_pct=zone_profile.noise_rh_pct if noise_rh_pct is None else float(noise_rh_pct),
            drift_temp_step_c=zone_profile.drift_temp_step_c if drift_temp_step_c is None else float(drift_temp_step_c),
            drift_rh_step_pct=zone_profile.drift_rh_step_pct if drift_rh_step_pct is None else float(drift_rh_step_pct),
            drift_temp_limit_c=max(
                zone_profile.drift_temp_limit_c,
                abs(float(zone_profile.drift_temp_step_c if drift_temp_step_c is None else drift_temp_step_c)) * 12.0,
            ),
            drift_rh_limit_pct=max(
                zone_profile.drift_rh_limit_pct,
                abs(float(zone_profile.drift_rh_step_pct if drift_rh_step_pct is None else drift_rh_step_pct)) * 12.0,
            ),
            event_rate_per_hour=zone_profile.event_rate_per_hour if event_rate_per_hour is None else float(event_rate_per_hour),
            event_rate_active_hours_per_hour=(
                zone_profile.event_rate_active_hours_per_hour
                if event_rate_active_hours_per_hour is None
                else float(event_rate_active_hours_per_hour)
            ),
            event_spike_temp_c=zone_profile.event_spike_temp_c if event_spike_temp_c is None else float(event_spike_temp_c),
            event_spike_rh_pct=zone_profile.event_spike_rh_pct if event_spike_rh_pct is None else float(event_spike_rh_pct),
            recovery_tau_seconds=(
                zone_profile.recovery_tau_seconds if recovery_tau_seconds is None else float(recovery_tau_seconds)
            ),
            baseline_reversion_tau_seconds=(
                zone_profile.baseline_reversion_tau_seconds
                if baseline_reversion_tau_seconds is None
                else float(baseline_reversion_tau_seconds)
            ),
            seasonal_temp_weight=(
                zone_profile.seasonal_temp_weight if seasonal_temp_weight is None else float(seasonal_temp_weight)
            ),
            seasonal_rh_weight=(
                zone_profile.seasonal_rh_weight if seasonal_rh_weight is None else float(seasonal_rh_weight)
            ),
            diurnal_temp_weight=(
                zone_profile.diurnal_temp_weight if diurnal_temp_weight is None else float(diurnal_temp_weight)
            ),
            diurnal_rh_weight=(
                zone_profile.diurnal_rh_weight if diurnal_rh_weight is None else float(diurnal_rh_weight)
            ),
            timezone_name=timezone_name,
            start_local_time=resolved_start_local_time,
        )
        return cls(config=config, schedule=schedule or ActiveHoursSchedule(), rng=rng or random.Random())
    # Method purpose: Generate the next telemetry sample for this synthetic
    #   warehouse zone.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns GeneratedTelemetrySample when the function completes
    #   successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def next_sample(self) -> GeneratedTelemetrySample:
        """Generate the next telemetry sample for this synthetic warehouse zone."""
        dt = float(self.config.interval_s)
        self.seq += 1
        self.uptime_s += dt

        schedule_hour_offset = self._schedule_hour_offset()
        active_hours = self.schedule.is_active(self.uptime_s, start_hour_offset=schedule_hour_offset)
        target = self._current_target(active_hours)

        self._update_drift(dt, target.indoor_temp_c, target.indoor_rh_pct)
        self._recover_disturbance(dt)

        event_rate = self.schedule.event_rate_per_hour(
            base_rate_per_hour=self.config.event_rate_per_hour,
            active_rate_per_hour=self.config.event_rate_active_hours_per_hour,
            uptime_s=self.uptime_s,
            start_hour_offset=schedule_hour_offset,
        )
        noise_multiplier = self.schedule.noise_multiplier(self.uptime_s, start_hour_offset=schedule_hour_offset)

        baseline_temp_c = self.config.base_temp_c + self.temp_drift_c
        baseline_rh_pct = self.config.base_rh_pct + self.rh_drift_pct
        disturbance_just_triggered = self._maybe_trigger_event(
            dt,
            event_rate,
            baseline_temp_c=baseline_temp_c,
            baseline_rh_pct=baseline_rh_pct,
            outdoor_temp_c=target.outdoor_temp_c,
            outdoor_rh_pct=target.outdoor_rh_pct,
        )

        temp_c = self._clamp(
            baseline_temp_c + self.disturbance_temp_c + self.rng.gauss(0.0, self.config.noise_temp_c * noise_multiplier),
            self.config.temp_min_c,
            self.config.temp_max_c,
        )
        rh_pct = self._clamp(
            baseline_rh_pct + self.disturbance_rh_pct + self.rng.gauss(0.0, self.config.noise_rh_pct * noise_multiplier),
            self.config.rh_min_pct,
            self.config.rh_max_pct,
        )

        disturbance_active = disturbance_just_triggered or (
            abs(self.disturbance_temp_c) >= self.config.disturbance_temp_threshold_c
            or abs(self.disturbance_rh_pct) >= self.config.disturbance_rh_threshold_pct
        )

        return GeneratedTelemetrySample(
            pod_id=self.config.pod_id,
            seq=self.seq,
            ts_uptime_s=self.uptime_s,
            temp_c=temp_c,
            rh_pct=rh_pct,
            flags=self.config.flags,
            zone_profile=self.config.zone_profile_name,
            disturbance_active=disturbance_active,
            disturbance_just_triggered=disturbance_just_triggered,
            active_hours=active_hours,
            baseline_temp_c=baseline_temp_c,
            baseline_rh_pct=baseline_rh_pct,
        )
    # Method purpose: Implements the update drift step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as dt, target_temp_c, target_rh_pct, interpreted
    #   according to the rules encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _update_drift(self, dt: float, target_temp_c: float, target_rh_pct: float) -> None:
        target_temp_drift_c = target_temp_c - self.config.base_temp_c
        target_rh_drift_pct = target_rh_pct - self.config.base_rh_pct
        reversion = 1.0 - math.exp(-dt / max(dt, self.config.baseline_reversion_tau_seconds))
        self.temp_drift_c = self._clamp(
            self.temp_drift_c
            + (reversion * (target_temp_drift_c - self.temp_drift_c))
            + self.rng.gauss(0.0, self.config.drift_temp_step_c),
            -self.config.drift_temp_limit_c,
            self.config.drift_temp_limit_c,
        )
        self.rh_drift_pct = self._clamp(
            self.rh_drift_pct
            + (reversion * (target_rh_drift_pct - self.rh_drift_pct))
            + self.rng.gauss(0.0, self.config.drift_rh_step_pct),
            -self.config.drift_rh_limit_pct,
            self.config.drift_rh_limit_pct,
        )
    # Method purpose: Implements the recover disturbance step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as dt, interpreted according to the rules encoded
    #   in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _recover_disturbance(self, dt: float) -> None:
        tau = max(1.0, float(self.config.recovery_tau_seconds))
        factor = math.exp(-dt / tau)
        self.disturbance_temp_c *= factor
        self.disturbance_rh_pct *= factor
    # Method purpose: Implements the maybe trigger event step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as dt, event_rate_per_hour, baseline_temp_c,
    #   baseline_rh_pct, outdoor_temp_c, outdoor_rh_pct, interpreted according
    #   to the rules encoded in the body below.
    # - Outputs: Returns bool when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _maybe_trigger_event(
        self,
        dt: float,
        event_rate_per_hour: float,
        *,
        baseline_temp_c: float,
        baseline_rh_pct: float,
        outdoor_temp_c: float,
        outdoor_rh_pct: float,
    ) -> bool:
        if event_rate_per_hour <= 0:
            return False
        event_probability = 1.0 - math.exp(-(event_rate_per_hour * dt / 3600.0))
        if self.rng.random() >= event_probability:
            return False

        temp_gap_c = outdoor_temp_c - baseline_temp_c
        rh_gap_pct = outdoor_rh_pct - baseline_rh_pct
        temp_direction = 1.0 if temp_gap_c >= 0.0 else -1.0
        rh_direction = 1.0 if rh_gap_pct >= 0.0 else -1.0
        temp_scale = min(1.15, 0.35 + (abs(temp_gap_c) / 5.0))
        rh_scale = min(1.15, 0.35 + (abs(rh_gap_pct) / 10.0))

        self.disturbance_temp_c += (
            temp_direction * self.config.event_spike_temp_c * temp_scale * self.rng.uniform(0.90, 1.10)
        )
        self.disturbance_rh_pct += (
            rh_direction * self.config.event_spike_rh_pct * rh_scale * self.rng.uniform(0.90, 1.10)
        )
        return True
    # Method purpose: Implements the current target step used by this subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as active_hours, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns IndoorClimateTarget when the function completes
    #   successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _current_target(self, active_hours: bool) -> IndoorClimateTarget:
        local_time = self.config.start_local_time + timedelta(seconds=self.uptime_s)
        target = bristol_indoor_target(
            local_time,
            base_temp_c=self.config.base_temp_c,
            base_rh_pct=self.config.base_rh_pct,
            seasonal_temp_weight=self.config.seasonal_temp_weight,
            seasonal_rh_weight=self.config.seasonal_rh_weight,
            diurnal_temp_weight=self.config.diurnal_temp_weight,
            diurnal_rh_weight=self.config.diurnal_rh_weight,
        )
        if not active_hours:
            return target
        return IndoorClimateTarget(
            indoor_temp_c=target.indoor_temp_c + 0.12,
            indoor_rh_pct=target.indoor_rh_pct + 0.35,
            outdoor_temp_c=target.outdoor_temp_c,
            outdoor_rh_pct=target.outdoor_rh_pct,
        )
    # Method purpose: Implements the schedule hour offset step used by this
    #   subsystem.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    def _schedule_hour_offset(self) -> float:
        start = self.config.start_local_time
        return start.hour + (start.minute / 60.0) + (start.second / 3600.0)
    # Method purpose: Constrains clamp to the safe range expected by later
    #   logic.
    # - Project role: Belongs to the synthetic pod simulation layer and acts as
    #   a method on SyntheticTelemetryGenerator.
    # - Inputs: Arguments such as value, lower, upper, interpreted according to
    #   the rules encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Simulation logic needs explicit assumptions because
    #   generated telemetry is later interpreted as if it were a real pod
    #   stream.
    # - Related flow: Produces synthetic telemetry and fault patterns for
    #   gateway and dashboard exercise.

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))
# Function purpose: Resolves zoneinfo into the concrete value used later.
# - Project role: Belongs to the synthetic pod simulation layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as timezone_name, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns ZoneInfo | None when the function completes successfully.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

def _resolve_zoneinfo(timezone_name: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return None
# Function purpose: Implements the default start local time step used by this
#   subsystem.
# - Project role: Belongs to the synthetic pod simulation layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as zoneinfo, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Simulation logic needs explicit assumptions because
#   generated telemetry is later interpreted as if it were a real pod stream.
# - Related flow: Produces synthetic telemetry and fault patterns for gateway and
#   dashboard exercise.

def _default_start_local_time(zoneinfo: ZoneInfo | None) -> datetime:
    if zoneinfo is None:
        return datetime.now()
    return datetime.now(zoneinfo)
