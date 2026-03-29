from __future__ import annotations

import csv
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.forecast.runner import ForecastRunner


class ForecastRunnerTests(unittest.TestCase):
    def test_csv_runner_writes_forecast_evaluation_and_case_base(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            pod_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(300):
                    ts_value = start + timedelta(minutes=index)
                    temp_c = 20.0 + 0.01 * index
                    rh_pct = 45.0 + 0.02 * index
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), f"{temp_c:.2f}", f"{rh_pct:.2f}", ""])

            runner = ForecastRunner(storage_backend="csv", data_root=data_root, k=2)
            requested_time = datetime(2026, 3, 28, 3, 0, tzinfo=timezone.utc)
            bundles = runner.run_cycle(pod_ids=["01"], requested_time_utc=requested_time)

            self.assertEqual(len(bundles), 1)
            self.assertEqual(len(bundles[0].baseline.temp_forecast_c), 30)

            evaluations = runner.evaluate_due(
                now_utc=requested_time + timedelta(minutes=30),
                pod_ids=["01"],
            )

            self.assertTrue(evaluations)
            self.assertTrue((data_root / "ml" / "forecasts.jsonl").exists())
            self.assertTrue((data_root / "ml" / "evaluations.jsonl").exists())
            self.assertTrue((data_root / "ml" / "case_base.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
