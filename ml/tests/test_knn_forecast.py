from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import build_config
from forecasting.features import extract_feature_vector
from forecasting.knn_forecaster import AnalogueKNNForecaster
from forecasting.models import CaseRecord


class KnnForecastTests(unittest.TestCase):
    def test_knn_forecast_returns_horizon_arrays_and_percentiles(self) -> None:
        baseline_window = synthetic_window()
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=2, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        cases = [
            CaseRecord(
                ts_pc_utc="2026-03-27T00:00:00Z",
                pod_id="01",
                feature_vector={key: value for key, value in feature_vector.values.items()},
                future_temp_c=[20.0 + 0.1 * index for index in range(30)],
                future_rh_pct=[40.0 + 0.2 * index for index in range(30)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-27T00:30:00Z",
                pod_id="01",
                feature_vector={key: value + 0.01 for key, value in feature_vector.values.items()},
                future_temp_c=[22.0 + 0.1 * index for index in range(30)],
                future_rh_pct=[44.0 + 0.2 * index for index in range(30)],
            ),
        ]

        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertEqual(forecast.source, "analogue_knn")
        self.assertEqual(len(forecast.temp_forecast_c), 30)
        self.assertEqual(len(forecast.rh_forecast_pct), 30)
        self.assertEqual(len(forecast.temp_p25_c), 30)
        self.assertEqual(len(forecast.rh_p75_pct), 30)
        self.assertAlmostEqual(forecast.temp_forecast_c[0], 21.0, places=6)
        self.assertLessEqual(forecast.temp_p25_c[0], forecast.temp_forecast_c[0])
        self.assertGreaterEqual(forecast.temp_p75_c[0], forecast.temp_forecast_c[0])


if __name__ == "__main__":
    unittest.main()
