# File overview:
# - Responsibility: Provides regression coverage for telemetry adjustments behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

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
# Class purpose: Groups related regression checks for DashboardTelemetryAdjustment
#   behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class DashboardTelemetryAdjustmentTests(unittest.TestCase):
    # Test purpose: Verifies that calibration offsets adjust values and
    #   recompute dew point behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardTelemetryAdjustmentTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
    # Test purpose: Verifies that dashboard smoothing reduces transient spike
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on DashboardTelemetryAdjustmentTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
