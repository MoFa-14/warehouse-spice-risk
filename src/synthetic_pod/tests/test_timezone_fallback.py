# File overview:
# - Responsibility: Provides regression coverage for timezone fallback behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

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
# Class purpose: Groups related regression checks for TimezoneFallback behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class TimezoneFallbackTests(unittest.TestCase):
    # Test purpose: Verifies that CLI accepts ianna timezone when zoneinfo
    #   database is missing behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on TimezoneFallbackTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_cli_accepts_ianna_timezone_when_zoneinfo_database_is_missing(self) -> None:
        with patch("pod2_sim.ZoneInfo", side_effect=ZoneInfoNotFoundError("missing tzdata")):
            self.assertEqual(pod2_sim._validate_timezone("Europe/London"), "Europe/London")
    # Test purpose: Verifies that CLI still rejects obviously invalid timezone
    #   names behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on TimezoneFallbackTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_cli_still_rejects_obviously_invalid_timezone_names(self) -> None:
        with patch("pod2_sim.ZoneInfo", side_effect=ZoneInfoNotFoundError("missing tzdata")):
            with self.assertRaises(argparse.ArgumentTypeError):
                pod2_sim._validate_timezone("London")
    # Test purpose: Verifies that generator uses fallback clock when zoneinfo
    #   database is missing behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on TimezoneFallbackTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
