from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.generator import SyntheticTelemetryGenerator
from sim.schedule import ActiveHoursSchedule
from sim.zone_profiles import get_zone_profile


class _DeterministicRng:
    def __init__(self, random_values: list[float] | None = None, uniform_values: list[float] | None = None) -> None:
        self._random_values = list(random_values or [])
        self._uniform_values = list(uniform_values or [])

    def random(self) -> float:
        if self._random_values:
            return self._random_values.pop(0)
        return 1.0

    def uniform(self, lower: float, upper: float) -> float:
        if self._uniform_values:
            return self._uniform_values.pop(0)
        return (lower + upper) / 2.0

    def gauss(self, _mu: float, _sigma: float) -> float:
        return 0.0


class EventRecoveryTests(unittest.TestCase):
    def test_event_spike_then_recovers_toward_baseline(self) -> None:
        rng = _DeterministicRng(
            random_values=[0.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            uniform_values=[1.0, 1.0],
        )
        generator = SyntheticTelemetryGenerator.from_zone_profile(
            pod_id="02",
            interval_s=10,
            zone_profile=get_zone_profile("entrance_disturbed"),
            noise_temp_c=0.0,
            noise_rh_pct=0.0,
            drift_temp_step_c=0.0,
            drift_rh_step_pct=0.0,
            event_rate_per_hour=5000.0,
            event_rate_active_hours_per_hour=5000.0,
            event_spike_temp_c=2.0,
            event_spike_rh_pct=8.0,
            recovery_tau_seconds=20.0,
            start_local_time=datetime(2026, 3, 29, 12, 0, 0),
            schedule=ActiveHoursSchedule(active_start_hour=12, active_end_hour=13),
            rng=rng,
        )

        first = generator.next_sample()
        second = generator.next_sample()
        third = generator.next_sample()

        self.assertTrue(first.disturbance_just_triggered)
        self.assertNotEqual(first.temp_c, first.baseline_temp_c)
        self.assertNotEqual(first.rh_pct, first.baseline_rh_pct)
        self.assertLess(abs(second.temp_c - second.baseline_temp_c), abs(first.temp_c - first.baseline_temp_c))
        self.assertLess(abs(second.rh_pct - second.baseline_rh_pct), abs(first.rh_pct - first.baseline_rh_pct))
        self.assertLess(abs(third.temp_c - third.baseline_temp_c), abs(second.temp_c - second.baseline_temp_c))
        self.assertLess(abs(third.rh_pct - third.baseline_rh_pct), abs(second.rh_pct - second.baseline_rh_pct))


if __name__ == "__main__":
    unittest.main()
