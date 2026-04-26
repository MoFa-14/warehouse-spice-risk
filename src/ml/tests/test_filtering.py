from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import ForecastConfig
from forecasting.event_detection import detect_recent_event
from forecasting.filtering import build_baseline_window


class FilteringTests(unittest.TestCase):
    def test_baseline_filter_reduces_spike_influence(self) -> None:
        window = synthetic_window(temp_rate_per_min=0.0, rh_rate_per_min=0.0)
        for index in range(175, 180):
            window[index] = window[index].__class__(
                ts_utc=window[index].ts_utc,
                temp_c=window[index].temp_c + 0.8 * (index - 174),
                rh_pct=window[index].rh_pct + 0.3 * (index - 174),
                dew_point_c=window[index].dew_point_c,
                observed=True,
            )

        config = ForecastConfig()
        detection = detect_recent_event(window, config=config)
        filtered = build_baseline_window(window, detection=detection, config=config)

        self.assertTrue(detection.event_detected)
        self.assertLess(filtered[-1].temp_c, window[-1].temp_c)
        self.assertLess(abs(filtered[-1].temp_c - filtered[-6].temp_c), abs(window[-1].temp_c - window[-6].temp_c))


if __name__ == "__main__":
    unittest.main()
