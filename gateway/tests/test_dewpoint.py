from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.preprocess.dewpoint import dew_point_c


class DewPointTests(unittest.TestCase):
    def test_magnus_formula_matches_expected_value(self) -> None:
        self.assertAlmostEqual(dew_point_c(20.0, 50.0) or 0.0, 9.26, places=1)

    def test_returns_none_for_missing_or_zero_humidity(self) -> None:
        self.assertIsNone(dew_point_c(None, 50.0))
        self.assertIsNone(dew_point_c(20.0, None))
        self.assertIsNone(dew_point_c(20.0, 0.0))


if __name__ == "__main__":
    unittest.main()
