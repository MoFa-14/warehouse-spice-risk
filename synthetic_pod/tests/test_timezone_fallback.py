from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfoNotFoundError

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

import pod2_sim
from sim.generator import SyntheticTelemetryGenerator
from sim.zone_profiles import get_zone_profile


class TimezoneFallbackTests(unittest.TestCase):
    def test_cli_accepts_ianna_timezone_when_zoneinfo_database_is_missing(self) -> None:
        with patch("pod2_sim.ZoneInfo", side_effect=ZoneInfoNotFoundError("missing tzdata")):
            self.assertEqual(pod2_sim._validate_timezone("Europe/London"), "Europe/London")

    def test_cli_still_rejects_obviously_invalid_timezone_names(self) -> None:
        with patch("pod2_sim.ZoneInfo", side_effect=ZoneInfoNotFoundError("missing tzdata")):
            with self.assertRaises(argparse.ArgumentTypeError):
                pod2_sim._validate_timezone("London")

    def test_generator_uses_fallback_clock_when_zoneinfo_database_is_missing(self) -> None:
        with patch("sim.generator.ZoneInfo", side_effect=ZoneInfoNotFoundError("missing tzdata")):
            generator = SyntheticTelemetryGenerator.from_zone_profile(
                pod_id="02",
                interval_s=60,
                zone_profile=get_zone_profile("entrance_disturbed"),
                timezone_name="Europe/London",
            )

        self.assertIsNone(generator.config.start_local_time.tzinfo)


if __name__ == "__main__":
    unittest.main()
