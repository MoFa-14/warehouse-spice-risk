from __future__ import annotations

import sys
import unittest
from math import isclose
from pathlib import Path

import pandas as pd

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.telemetry_adjustments import (
    PodCalibration,
    SmoothingSettings,
    TelemetryAdjustments,
    apply_calibration,
    apply_smoothing,
)


class DashboardTelemetryAdjustmentTests(unittest.TestCase):
    def test_calibration_offsets_adjust_values_and_recompute_dew_point(self) -> None:
        frame = pd.DataFrame(
            [
                {"ts_pc_utc": pd.Timestamp("2026-03-29T12:00:00Z"), "pod_id": "01", "temp_c": 20.0, "rh_pct": 50.0, "dew_point_c": 9.26},
            ]
        )
        adjustments = TelemetryAdjustments(pods={"01": PodCalibration(temp_offset_c=1.5, rh_offset_pct=-5.0)})

        adjusted = apply_calibration(frame, temp_column="temp_c", rh_column="rh_pct", adjustments=adjustments)

        self.assertAlmostEqual(float(adjusted.iloc[0]["temp_c"]), 21.5)
        self.assertAlmostEqual(float(adjusted.iloc[0]["rh_pct"]), 45.0)
        self.assertFalse(isclose(float(adjusted.iloc[0]["dew_point_c"]), 9.26, rel_tol=0.0, abs_tol=1e-6))

    def test_dashboard_smoothing_reduces_transient_spike(self) -> None:
        frame = pd.DataFrame(
            [
                {"ts_pc_utc": pd.Timestamp("2026-03-29T12:00:00Z"), "pod_id": "01", "temp_c": 20.0, "rh_pct": 45.0},
                {"ts_pc_utc": pd.Timestamp("2026-03-29T12:01:00Z"), "pod_id": "01", "temp_c": 20.0, "rh_pct": 45.0},
                {"ts_pc_utc": pd.Timestamp("2026-03-29T12:02:00Z"), "pod_id": "01", "temp_c": 28.0, "rh_pct": 45.0},
            ]
        )

        smoothed = apply_smoothing(
            frame,
            value_columns=("temp_c", "rh_pct"),
            settings=SmoothingSettings(enabled=True, window=3),
        )

        self.assertLess(float(smoothed.iloc[-1]["temp_c"]), 28.0)
        self.assertAlmostEqual(float(smoothed.iloc[-1]["temp_c"]), 22.666666, places=4)


if __name__ == "__main__":
    unittest.main()
