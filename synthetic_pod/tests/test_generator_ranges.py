from __future__ import annotations

import sys
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
