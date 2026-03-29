from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
import random

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.generator import SyntheticTelemetryGenerator
from sim.zone_profiles import ZONE_PROFILES


class GeneratorRangeTests(unittest.TestCase):
    def test_zone_profiles_stay_within_plausible_bounds(self) -> None:
        for zone in ZONE_PROFILES.values():
            generator = SyntheticTelemetryGenerator.from_zone_profile(
                pod_id="02",
                interval_s=10,
                zone_profile=zone,
            )
            for _ in range(1000):
                sample = generator.next_sample()
                self.assertGreaterEqual(sample.temp_c, -5.0, zone.name)
                self.assertLessEqual(sample.temp_c, 45.0, zone.name)
                self.assertGreaterEqual(sample.rh_pct, 0.0, zone.name)
                self.assertLessEqual(sample.rh_pct, 100.0, zone.name)

    def test_entrance_profile_changes_stay_gentle_without_forced_events(self) -> None:
        generator = SyntheticTelemetryGenerator.from_zone_profile(
            pod_id="02",
            interval_s=60,
            zone_profile=ZONE_PROFILES["entrance_disturbed"],
            event_rate_per_hour=0.0,
            event_rate_active_hours_per_hour=0.0,
            start_local_time=datetime(2026, 3, 29, 2, 0, 0),
            rng=random.Random(7),
        )

        samples = [generator.next_sample() for _ in range(180)]
        temp_deltas = [abs(curr.temp_c - prev.temp_c) for prev, curr in zip(samples, samples[1:])]
        rh_deltas = [abs(curr.rh_pct - prev.rh_pct) for prev, curr in zip(samples, samples[1:])]

        self.assertLess(max(temp_deltas), 1.0)
        self.assertLess(max(rh_deltas), 3.0)
        self.assertGreater(sum(temp_deltas) / len(temp_deltas), 0.02)
        self.assertGreater(sum(rh_deltas) / len(rh_deltas), 0.08)


if __name__ == "__main__":
    unittest.main()
