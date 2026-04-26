from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.weather import bristol_indoor_target


class WeatherTrendTests(unittest.TestCase):
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
