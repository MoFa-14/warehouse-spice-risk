# File overview:
# - Responsibility: Provides regression coverage for weather trend behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.weather import bristol_indoor_target
# Class purpose: Groups related regression checks for WeatherTrend behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class WeatherTrendTests(unittest.TestCase):
    # Test purpose: Verifies that summer target is warmer than winter target
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WeatherTrendTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_summer_target_is_warmer_than_winter_target(self) -> None:
        winter = bristol_indoor_target(
            datetime(2026, 1, 15, 14, 0, 0),
            base_temp_c=18.4,
            base_rh_pct=52.0,
            seasonal_temp_weight=0.42,
            seasonal_rh_weight=0.32,
            diurnal_temp_weight=0.26,
            diurnal_rh_weight=0.18,
        )
        summer = bristol_indoor_target(
            datetime(2026, 7, 15, 14, 0, 0),
            base_temp_c=18.4,
            base_rh_pct=52.0,
            seasonal_temp_weight=0.42,
            seasonal_rh_weight=0.32,
            diurnal_temp_weight=0.26,
            diurnal_rh_weight=0.18,
        )

        self.assertGreater(summer.indoor_temp_c, winter.indoor_temp_c)
    # Test purpose: Verifies that daytime target is warmer and drier than pre
    #   dawn behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on WeatherTrendTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_daytime_target_is_warmer_and_drier_than_pre_dawn(self) -> None:
        predawn = bristol_indoor_target(
            datetime(2026, 3, 29, 4, 0, 0),
            base_temp_c=18.4,
            base_rh_pct=52.0,
            seasonal_temp_weight=0.42,
            seasonal_rh_weight=0.32,
            diurnal_temp_weight=0.26,
            diurnal_rh_weight=0.18,
        )
        afternoon = bristol_indoor_target(
            datetime(2026, 3, 29, 15, 0, 0),
            base_temp_c=18.4,
            base_rh_pct=52.0,
            seasonal_temp_weight=0.42,
            seasonal_rh_weight=0.32,
            diurnal_temp_weight=0.26,
            diurnal_rh_weight=0.18,
        )

        self.assertGreater(afternoon.indoor_temp_c, predawn.indoor_temp_c)
        self.assertLess(afternoon.indoor_rh_pct, predawn.indoor_rh_pct)
