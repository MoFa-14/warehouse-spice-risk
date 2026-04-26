from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import ForecastConfig
from forecasting.scenario import build_event_persist_forecast


class EventPersistScenarioTests(unittest.TestCase):
    def test_event_persist_rh_uses_decay_and_total_drift_cap(self) -> None:
        window = synthetic_window(
            temp_base=24.0,
            rh_base=39.04,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        rh_tail = [39.04, 37.10, 36.64, 35.74, 35.29, 35.52]
        for offset, rh_value in enumerate(rh_tail, start=len(window) - len(rh_tail)):
            point = window[offset]
            window[offset] = point.__class__(
                ts_utc=point.ts_utc,
                temp_c=point.temp_c,
                rh_pct=rh_value,
                dew_point_c=point.dew_point_c,
                observed=True,
            )

        forecast = build_event_persist_forecast(window, config=ForecastConfig())

        self.assertEqual(forecast.source, "event_persist_slope")
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 34.816, places=3)
        self.assertGreater(forecast.rh_forecast_pct[9], 31.0)
        self.assertGreaterEqual(min(forecast.rh_forecast_pct), 31.52 - 1e-6)
        first_drop = forecast.rh_forecast_pct[0] - forecast.rh_forecast_pct[1]
        later_drop = forecast.rh_forecast_pct[5] - forecast.rh_forecast_pct[6]
        self.assertGreater(first_drop, later_drop)
        self.assertIn("exponential RH decay", forecast.notes)


if __name__ == "__main__":
    unittest.main()
