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
from forecasting.models import EvaluationMetrics


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
            self.assertIsNotNone(evaluations[0].persistence_rmse_temp_c)
            self.assertIsNotNone(evaluations[0].persistence_rmse_rh_pct)
            self.assertTrue((data_root / "ml" / "forecasts.jsonl").exists())
            self.assertTrue((data_root / "ml" / "evaluations.jsonl").exists())
            self.assertTrue((data_root / "ml" / "case_base.jsonl").exists())

    def test_forecast_pod_uses_full_three_hour_history_window_and_latest_observation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            pod_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(181):
                    ts_value = start + timedelta(minutes=index)
                    temp_c = 18.0 + 0.05 * index
                    rh_pct = 55.0 - 0.03 * index
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), f"{temp_c:.2f}", f"{rh_pct:.2f}", ""])

            runner = ForecastRunner(storage_backend="csv", data_root=data_root, k=2)
            requested_time = start + timedelta(minutes=180)
            bundle = runner.forecast_pod(pod_id="01", requested_time_utc=requested_time)

            self.assertIsNotNone(bundle)
            assert bundle is not None
            self.assertIn("01", runner._buffers)
            self.assertEqual(len(runner._buffers["01"]), 180)
            self.assertEqual(
                runner._buffers["01"][0].ts_utc.isoformat().replace("+00:00", "Z"),
                "2026-03-28T00:01:00Z",
            )
            self.assertEqual(
                runner._buffers["01"][-1].ts_utc.isoformat().replace("+00:00", "Z"),
                "2026-03-28T03:00:00Z",
            )
            self.assertAlmostEqual(bundle.feature_vector.values["temp_last"], 27.0, places=6)
            self.assertAlmostEqual(bundle.feature_vector.values["rh_last"], 49.6, places=6)

    def test_runner_auto_backfills_cases_and_calibrates_from_recent_evaluations(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            pod_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(420):
                    ts_value = start + timedelta(minutes=index)
                    temp_c = 19.5 + 0.01 * index
                    rh_pct = 48.0 + 0.015 * index
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), f"{temp_c:.2f}", f"{rh_pct:.2f}", ""])

            runner = ForecastRunner(storage_backend="csv", data_root=data_root, k=2)
            requested_time = datetime(2026, 3, 28, 4, 0, tzinfo=timezone.utc)
            bundles = runner.run_cycle(pod_ids=["01"], requested_time_utc=requested_time)

            self.assertEqual(len(bundles), 1)
            learned_cases = runner.case_base.load_cases(pod_id="01", include_event_cases=True)
            self.assertGreaterEqual(len(learned_cases), 2)

            uncalibrated_temp = bundles[0].baseline.temp_forecast_c[0]
            uncalibrated_rh = bundles[0].baseline.rh_forecast_pct[0]
            for offset_minutes in (90, 60, 30):
                runner.outputs.save_evaluation(
                    EvaluationMetrics(
                        ts_forecast_utc=(requested_time - timedelta(minutes=offset_minutes)).isoformat().replace("+00:00", "Z"),
                        pod_id="01",
                        scenario="baseline",
                        mae_temp_c=0.30,
                        rmse_temp_c=0.45,
                        mae_rh_pct=1.50,
                        rmse_rh_pct=2.10,
                        bias_temp_c=0.60,
                        bias_rh_pct=2.50,
                        event_detected=False,
                        large_error=False,
                        notes="ok",
                    )
                )

            calibrated = runner.forecast_pod(pod_id="01", requested_time_utc=requested_time)

            self.assertIsNotNone(calibrated)
            assert calibrated is not None
            self.assertAlmostEqual(calibrated.baseline.temp_forecast_c[0], uncalibrated_temp - 0.60, places=3)
            self.assertAlmostEqual(calibrated.baseline.rh_forecast_pct[0], uncalibrated_rh - 2.50, places=3)
            self.assertIn("Auto-calibrated using 3 recent evaluations", calibrated.baseline.notes)

    def test_recent_bias_ignores_large_error_and_incomplete_windows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            runner = ForecastRunner(storage_backend="csv", data_root=data_root, k=2)

            for index in range(3):
                runner.outputs.save_evaluation(
                    EvaluationMetrics(
                        ts_forecast_utc=(datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
                        pod_id="01",
                        scenario="baseline",
                        mae_temp_c=0.20,
                        rmse_temp_c=0.30,
                        mae_rh_pct=0.80,
                        rmse_rh_pct=1.10,
                        bias_temp_c=0.20,
                        bias_rh_pct=-0.40,
                        event_detected=False,
                        large_error=False,
                        notes="ok",
                    )
                )
            for index in range(3, 6):
                runner.outputs.save_evaluation(
                    EvaluationMetrics(
                        ts_forecast_utc=(datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
                        pod_id="01",
                        scenario="baseline",
                        mae_temp_c=4.0,
                        rmse_temp_c=4.5,
                        mae_rh_pct=10.0,
                        rmse_rh_pct=12.0,
                        bias_temp_c=-3.0,
                        bias_rh_pct=8.0,
                        event_detected=False,
                        large_error=True,
                        notes="large_error;actual_missing_rate=0.500",
                    )
                )

            bias = runner.outputs.recent_bias(pod_id="01", scenario="baseline", limit=12)

            self.assertIsNotNone(bias)
            assert bias is not None
            self.assertAlmostEqual(bias.temp_c, 0.20, places=6)
            self.assertAlmostEqual(bias.rh_pct, -0.40, places=6)
            self.assertEqual(bias.sample_count, 3)

    def test_runner_backfills_missing_persistence_metrics_for_existing_evaluations(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            pod_dir = data_root / "raw" / "pods" / "01"
            pod_dir.mkdir(parents=True, exist_ok=True)
            csv_path = pod_dir / "2026-03-28.csv"
            start = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ts_pc_utc", "temp_c", "rh_pct", "dew_point_c"])
                for index in range(360):
                    ts_value = start + timedelta(minutes=index)
                    temp_c = 21.0 + 0.01 * index
                    rh_pct = 46.0 + 0.015 * index
                    writer.writerow([ts_value.isoformat().replace("+00:00", "Z"), f"{temp_c:.2f}", f"{rh_pct:.2f}", ""])

            runner = ForecastRunner(storage_backend="csv", data_root=data_root, k=2)
            requested_time = datetime(2026, 3, 28, 4, 0, tzinfo=timezone.utc)
            bundles = runner.run_cycle(pod_ids=["01"], requested_time_utc=requested_time)

            self.assertEqual(len(bundles), 1)
            runner.outputs.save_evaluation(
                EvaluationMetrics(
                    ts_forecast_utc=bundles[0].ts_pc_utc,
                    pod_id="01",
                    scenario="baseline",
                    mae_temp_c=0.25,
                    rmse_temp_c=0.35,
                    mae_rh_pct=0.90,
                    rmse_rh_pct=1.20,
                    bias_temp_c=0.05,
                    bias_rh_pct=-0.10,
                    event_detected=False,
                    large_error=False,
                    notes="legacy",
                )
            )

            backfilled = runner.backfill_persistence_metrics(
                now_utc=requested_time + timedelta(minutes=30),
                pod_ids=["01"],
            )

            self.assertEqual(backfilled, 1)
            evaluations = runner.outputs._read_jsonl(runner.outputs.evaluations_jsonl)
            self.assertEqual(len(evaluations), 1)
            self.assertIsNotNone(evaluations[0]["PERSIST_RMSE_T"])
            self.assertIsNotNone(evaluations[0]["PERSIST_RMSE_RH"])


if __name__ == "__main__":
    unittest.main()
