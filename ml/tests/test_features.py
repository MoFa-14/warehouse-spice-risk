from __future__ import annotations

import unittest

from _helpers import load_fixture_points, synthetic_window
from forecasting.features import extract_feature_vector


class FeatureExtractionTests(unittest.TestCase):
    def test_extract_features_includes_time_and_recent_trends(self) -> None:
        window = synthetic_window(temp_rate_per_min=0.05, rh_rate_per_min=-0.03)

        feature_vector = extract_feature_vector(window)

        self.assertIn("hour_sin", feature_vector.values)
        self.assertIn("hour_cos", feature_vector.values)
        self.assertGreater(feature_vector.values["temp_slope_30"], 0.0)
        self.assertLess(feature_vector.values["rh_slope_30"], 0.0)
        self.assertEqual(feature_vector.observed_points, 180)

    def test_fixture_file_loads_for_small_realistic_window(self) -> None:
        points = load_fixture_points()

        feature_vector = extract_feature_vector(points)

        self.assertEqual(len(points), 37)
        self.assertEqual(feature_vector.observed_points, 37)
        self.assertAlmostEqual(feature_vector.values["temp_last"], points[-1].temp_c, places=6)


if __name__ == "__main__":
    unittest.main()
