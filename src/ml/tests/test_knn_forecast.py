from __future__ import annotations

import unittest

from _helpers import synthetic_window
from forecasting.config import build_config
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.features import extract_feature_vector
from forecasting.knn_forecaster import AnalogueKNNForecaster, _aggregate_neighbors
from forecasting.models import CaseRecord
from forecasting.utils import rmse


class KnnForecastTests(unittest.TestCase):
    def test_knn_forecast_reanchors_neighbor_deltas_for_temperature_and_humidity(self) -> None:
        baseline_window = synthetic_window(
            temp_base=30.0,
            rh_base=50.0,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=2, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        case_a_features = dict(feature_vector.values)
        case_a_features["temp_last"] = 10.0
        case_a_features["rh_last"] = 50.0
        case_a_features["dew_last"] = float(feature_vector.values["dew_last"])

        case_b_features = dict(feature_vector.values)
        case_b_features["temp_last"] = 60.0
        case_b_features["rh_last"] = 50.0
        case_b_features["dew_last"] = float(feature_vector.values["dew_last"])

        cases = [
            CaseRecord(
                ts_pc_utc="2026-03-27T00:00:00Z",
                pod_id="01",
                feature_vector=case_a_features,
                future_temp_c=[11.0 + 0.2 * index for index in range(30)],
                future_rh_pct=[51.0 - 0.1 * index for index in range(30)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-27T00:30:00Z",
                pod_id="01",
                feature_vector=case_b_features,
                future_temp_c=[59.0 + 0.2 * index for index in range(30)],
                future_rh_pct=[49.0 - 0.1 * index for index in range(30)],
            ),
        ]

        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertEqual(forecast.source, "analogue_knn")
        self.assertEqual(len(forecast.temp_forecast_c), 30)
        self.assertEqual(len(forecast.rh_forecast_pct), 30)
        self.assertEqual(len(forecast.temp_p25_c), 30)
        self.assertEqual(len(forecast.rh_p75_pct), 30)
        self.assertAlmostEqual(forecast.temp_forecast_c[0], 30.0, places=6)
        self.assertAlmostEqual(forecast.temp_forecast_c[5], 31.0, places=6)
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 50.0, places=6)
        self.assertAlmostEqual(forecast.rh_forecast_pct[5], 49.5, places=6)
        self.assertLessEqual(forecast.temp_p25_c[0], forecast.temp_forecast_c[0])
        self.assertGreaterEqual(forecast.temp_p75_c[0], forecast.temp_forecast_c[0])

    def test_knn_forecast_avoids_absolute_level_jump_from_misaligned_neighbors(self) -> None:
        baseline_window = synthetic_window(
            temp_base=24.0,
            rh_base=33.0,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=2, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        low_case_features = dict(feature_vector.values)
        low_case_features["temp_last"] = 10.0
        low_case_features["rh_last"] = 33.0
        low_case_features["dew_last"] = float(feature_vector.values["dew_last"])

        high_case_features = dict(feature_vector.values)
        high_case_features["temp_last"] = 60.0
        high_case_features["rh_last"] = 33.0
        high_case_features["dew_last"] = float(feature_vector.values["dew_last"])

        cases = [
            CaseRecord(
                ts_pc_utc="2026-03-27T00:00:00Z",
                pod_id="01",
                feature_vector=low_case_features,
                future_temp_c=[10.5 for _ in range(30)],
                future_rh_pct=[33.25 for _ in range(30)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-27T00:30:00Z",
                pod_id="01",
                feature_vector=high_case_features,
                future_temp_c=[60.5 for _ in range(30)],
                future_rh_pct=[33.25 for _ in range(30)],
            ),
        ]

        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertAlmostEqual(forecast.temp_forecast_c[0], 24.5, places=6)
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 33.25, places=6)
        self.assertNotAlmostEqual(forecast.temp_forecast_c[0], 35.5, places=6)
        self.assertNotAlmostEqual(forecast.rh_forecast_pct[0], 55.25, places=6)

    def test_knn_forecast_rejects_wrong_rh_regime_and_blends_weak_support_toward_persistence(self) -> None:
        baseline_window = synthetic_window(
            temp_base=24.0,
            rh_base=35.0,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=2, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        good_case_features = dict(feature_vector.values)
        bad_case_features = dict(feature_vector.values)
        bad_case_features["rh_last"] = 48.0
        bad_case_features["dew_last"] = calculate_dew_point_c(float(feature_vector.values["temp_last"]), 48.0)

        cases = [
            CaseRecord(
                ts_pc_utc="2026-03-27T23:50:00Z",
                pod_id="01",
                feature_vector=good_case_features,
                future_temp_c=[24.1 for _ in range(30)],
                future_rh_pct=[35.2 for _ in range(30)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-01T23:50:00Z",
                pod_id="01",
                feature_vector=bad_case_features,
                future_temp_c=[24.1 for _ in range(30)],
                future_rh_pct=[58.0 for _ in range(30)],
            ),
        ]

        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertEqual(forecast.source, "analogue_knn_rh_blend")
        self.assertAlmostEqual(forecast.temp_forecast_c[0], 24.1, places=6)
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 35.15, places=6)
        self.assertLess(forecast.rh_forecast_pct[0], 36.0)
        self.assertIn("blended RH toward persistence", forecast.notes)

    def test_knn_forecast_prefers_more_recent_similar_case(self) -> None:
        baseline_window = synthetic_window(
            temp_base=24.0,
            rh_base=35.0,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=1, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        recent_case_features = dict(feature_vector.values)
        old_case_features = dict(feature_vector.values)
        cases = [
            CaseRecord(
                ts_pc_utc="2026-03-01T23:50:00Z",
                pod_id="01",
                feature_vector=old_case_features,
                future_temp_c=[24.0 for _ in range(30)],
                future_rh_pct=[36.8 for _ in range(30)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-27T23:50:00Z",
                pod_id="01",
                feature_vector=recent_case_features,
                future_temp_c=[24.0 for _ in range(30)],
                future_rh_pct=[35.2 for _ in range(30)],
            ),
        ]

        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertEqual(forecast.source, "analogue_knn")
        self.assertAlmostEqual(forecast.rh_forecast_pct[0], 35.2, places=6)

    def test_knn_forecast_falls_back_to_persistence_when_all_cases_are_wrong_rh_regime(self) -> None:
        baseline_window = synthetic_window(
            temp_base=24.0,
            rh_base=35.77,
            temp_rate_per_min=0.0,
            rh_rate_per_min=0.0,
        )
        feature_vector = extract_feature_vector(baseline_window)
        config = build_config(k=3, horizon_minutes=30)
        forecaster = AnalogueKNNForecaster(config=config)

        cases = [
            CaseRecord(
                ts_pc_utc="2026-04-01T23:03:00Z",
                pod_id="01",
                feature_vector={
                    **feature_vector.values,
                    "rh_last": 43.57,
                    "dew_last": calculate_dew_point_c(float(feature_vector.values["temp_last"]), 43.57),
                },
                future_temp_c=[24.0 for _ in range(30)],
                future_rh_pct=[42.47, 44.14, 43.28, 42.88, 42.77] + [42.7 for _ in range(25)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-29T15:57:00Z",
                pod_id="01",
                feature_vector={
                    **feature_vector.values,
                    "rh_last": 46.56,
                    "dew_last": calculate_dew_point_c(float(feature_vector.values["temp_last"]), 46.56),
                },
                future_temp_c=[24.0 for _ in range(30)],
                future_rh_pct=[46.35, 46.44, 46.65, 46.67, 46.56] + [46.5 for _ in range(25)],
            ),
            CaseRecord(
                ts_pc_utc="2026-03-29T16:27:00Z",
                pod_id="01",
                feature_vector={
                    **feature_vector.values,
                    "rh_last": 47.38,
                    "dew_last": calculate_dew_point_c(float(feature_vector.values["temp_last"]), 47.38),
                },
                future_temp_c=[24.0 for _ in range(30)],
                future_rh_pct=[48.09, 48.09, 48.15, 48.03, 47.82] + [48.0 for _ in range(25)],
            ),
        ]
        actual_rh = [
            35.86,
            35.63,
            35.67,
            35.77,
            35.67,
            35.81,
            36.06,
            36.05,
            36.13,
            36.10,
        ] + [36.0 for _ in range(20)]

        old_forecast = _aggregate_neighbors(
            cases,
            scenario="baseline",
            anchor_temp_c=float(feature_vector.values["temp_last"]),
            anchor_rh_pct=float(feature_vector.values["rh_last"]),
        )
        forecast = forecaster.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=cases)

        self.assertEqual(forecast.source, "persistence_support_fallback")
        self.assertTrue(all(abs(value - 35.77) < 1e-6 for value in forecast.rh_forecast_pct))
        self.assertLess(rmse(forecast.rh_forecast_pct, actual_rh), rmse(old_forecast.rh_forecast_pct, actual_rh))
        self.assertIn("Rejected all 3 analogue cases", forecast.notes)


if __name__ == "__main__":
    unittest.main()
