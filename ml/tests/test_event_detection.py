from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import ForecastConfig
from forecasting.event_detection import detect_recent_event


class EventDetectionTests(unittest.TestCase):
    def test_recent_rh_spike_is_detected_as_door_open_like(self) -> None:
        window = synthetic_window()
        for index in range(174, 180):
            window[index] = window[index].__class__(
                ts_utc=window[index].ts_utc,
                temp_c=window[index].temp_c + 0.05 * (index - 173),
                rh_pct=window[index].rh_pct + 2.0 * (index - 173),
                dew_point_c=window[index].dew_point_c + 0.4 * (index - 173),
                observed=True,
            )

        detection = detect_recent_event(window, config=ForecastConfig())

        self.assertTrue(detection.event_detected)
        self.assertEqual(detection.event_type, "door_open_like")
        self.assertIn("dRH5", detection.event_reason)

    def test_stable_window_does_not_trigger_event(self) -> None:
        detection = detect_recent_event(synthetic_window(), config=ForecastConfig())

        self.assertFalse(detection.event_detected)
        self.assertEqual(detection.event_type, "none")


if __name__ == "__main__":
    unittest.main()
