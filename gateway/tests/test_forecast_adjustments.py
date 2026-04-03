from __future__ import annotations

import csv
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from math import isclose
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.forecast.storage_adapter import ForecastStorageAdapter


class ForecastAdjustmentTests(unittest.TestCase):
    def test_calibration_applies_to_forecast_input_but_raw_csv_remains_preserved(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            config_dir = data_root / "config"
            pod_dir.mkdir(parents=True, exist_ok=True)
            config_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            config_path = config_dir / "telemetry_adjustments.json"
            config_path.write_text(
                json.dumps({"pods": {"01": {"temp_offset_c": 1.5, "rh_offset_pct": -5.0}}}),
                encoding="utf-8",
            )
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(181):
                    ts_value = start + timedelta(minutes=index)
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), "20.0", "50.0", ""])

            adapter = ForecastStorageAdapter(
                storage_backend="csv",
                data_root=data_root,
                adjustments_path=config_path,
            )
            window = adapter.load_history_window(
                pod_id="01",
                as_of_utc=start + timedelta(minutes=180),
                minutes=180,
            )

            self.assertEqual(len(window.points), 180)
            self.assertAlmostEqual(window.points[-1].temp_c, 21.5)
            self.assertAlmostEqual(window.points[-1].rh_pct, 45.0)
            self.assertFalse(isclose(window.points[-1].dew_point_c, 9.26, rel_tol=0.0, abs_tol=1e-6))
            self.assertIn("20.0,50.0", csv_path.read_text(encoding="utf-8"))

    def test_forecast_input_smoothing_reduces_spike(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            config_dir = data_root / "config"
            pod_dir.mkdir(parents=True, exist_ok=True)
            config_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            config_path = config_dir / "telemetry_adjustments.json"
            config_path.write_text(
                json.dumps({"forecast_smoothing": {"enabled": True, "method": "rolling_mean", "window": 3}}),
                encoding="utf-8",
            )
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(181):
                    ts_value = start + timedelta(minutes=index)
                    temp_c = 28.0 if index == 180 else 20.0
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), f"{temp_c:.1f}", "45.0", ""])

            adapter = ForecastStorageAdapter(
                storage_backend="csv",
                data_root=data_root,
                adjustments_path=config_path,
            )
            window = adapter.load_history_window(
                pod_id="01",
                as_of_utc=start + timedelta(minutes=180),
                minutes=180,
            )

            self.assertLess(window.points[-1].temp_c, 28.0)
            self.assertAlmostEqual(window.points[-1].temp_c, 22.666666, places=4)


if __name__ == "__main__":
    unittest.main()
