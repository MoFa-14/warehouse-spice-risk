# File overview:
# - Responsibility: Provides regression coverage for prediction service behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from datetime import timezone
from pathlib import Path

import pandas as pd

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.prediction_service import (
    _build_forecast_chart,
    _build_metric_summary_cards,
    _build_persistence_comparison_chart,
    _event_persist_from_history_frame,
    _rmse_advantage_series,
    _scenario_view,
)
# Class purpose: Groups related regression checks for PredictionService behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class PredictionServiceTests(unittest.TestCase):
    # Test purpose: Verifies that RMSE advantage series compares against
    #   persistence per window behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_rmse_advantage_series_compares_against_persistence_per_window(self) -> None:
        model_rmse = pd.Series([0.40, 0.20], dtype="float64")
        persistence_rmse = pd.Series([0.60, 0.25], dtype="float64")

        advantage = _rmse_advantage_series(model_rmse=model_rmse, persistence_rmse=persistence_rmse)

        self.assertEqual(list(advantage.round(3)), [0.2, 0.05])
    # Test purpose: Verifies that RMSE advantage series stays stable when
    #   persistence RMSE is tiny behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_rmse_advantage_series_stays_stable_when_persistence_rmse_is_tiny(self) -> None:
        model_rmse = pd.Series([0.12], dtype="float64")
        persistence_rmse = pd.Series([0.001], dtype="float64")

        advantage = _rmse_advantage_series(model_rmse=model_rmse, persistence_rmse=persistence_rmse)

        self.assertAlmostEqual(float(advantage.iloc[0]), -0.119, places=6)
    # Test purpose: Verifies that persistence comparison chart uses updated
    #   title labels and legend behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_persistence_comparison_chart_uses_updated_title_labels_and_legend(self) -> None:
        evaluation_history = pd.DataFrame(
            {
                "ts_forecast_utc": pd.to_datetime(
                    ["2026-03-28T00:00:00Z", "2026-03-28T00:30:00Z"],
                    utc=True,
                ),
                "RMSE_T": [0.45, 0.30],
                "RMSE_RH": [1.30, 1.10],
                "PERSIST_RMSE_T": [0.60, 0.35],
                "PERSIST_RMSE_RH": [1.50, 1.40],
            }
        )

        chart_html = _build_persistence_comparison_chart(
            evaluation_history=evaluation_history,
            display_timezone=timezone.utc,
        )

        self.assertIsNotNone(chart_html)
        assert chart_html is not None
        self.assertIn("Per-window forecast RMSE advantage vs persistence", chart_html)
        self.assertIn("Temperature RMSE advantage (C)", chart_html)
        self.assertIn("RH RMSE advantage (%)", chart_html)
        self.assertIn("Temperature vs persistence", chart_html)
        self.assertIn("RH vs persistence", chart_html)
        self.assertNotIn("Forecast accuracy improvement over time", chart_html)
        self.assertNotIn("Improvement vs earliest RMSE (%)", chart_html)
    # Test purpose: Verifies that persistence comparison chart ignores rows
    #   without completed baseline comparison behaves as expected under this
    #   regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_persistence_comparison_chart_ignores_rows_without_completed_baseline_comparison(self) -> None:
        evaluation_history = pd.DataFrame(
            {
                "ts_forecast_utc": pd.to_datetime(
                    ["2026-03-28T00:00:00Z", "2026-03-29T00:00:00Z"],
                    utc=True,
                ),
                "RMSE_T": [0.45, 0.30],
                "RMSE_RH": [1.30, 1.10],
                "PERSIST_RMSE_T": [0.60, None],
                "PERSIST_RMSE_RH": [1.50, None],
            }
        )

        chart_html = _build_persistence_comparison_chart(
            evaluation_history=evaluation_history,
            display_timezone=timezone.utc,
        )

        self.assertIsNotNone(chart_html)
        assert chart_html is not None
        self.assertIn("2026-03-28", chart_html)
        self.assertNotIn("2026-03-29", chart_html)
    # Test purpose: Verifies that scenario view builds user friendly summary
    #   copy behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_scenario_view_builds_user_friendly_summary_copy(self) -> None:
        row = pd.Series(
            {
                "scenario": "event_persist",
                "json_forecast": (
                    '{"temp_forecast_c":[25.4,25.7,26.1],'
                    '"rh_forecast_pct":[65.2,66.1,67.0],'
                    '"dew_point_forecast_c":[18.2,18.9,19.6],'
                    '"feature_vector":{"temp_last":25.2,"rh_last":65.0,"dew_last":18.0},'
                    '"source":"event_persist_slope",'
                    '"neighbor_count":0,'
                    '"case_count":0,'
                    '"notes":"Continues the current event slope."}'
                ),
                "json_p25": '{"temp_c":[25.2,25.5,25.8],"rh_pct":[64.8,65.6,66.4]}',
                "json_p75": '{"temp_c":[25.6,25.9,26.4],"rh_pct":[65.6,66.5,67.4]}',
                "event_type": "door_open_like",
                "MAE_T": None,
                "RMSE_T": None,
                "MAE_RH": None,
                "RMSE_RH": None,
                "large_error": None,
                "evaluation_notes": "",
            }
        )

        scenario = _scenario_view(row)

        self.assertEqual(scenario.dew_start_c, 18.0)
        self.assertEqual(scenario.dew_peak_c, 19.6)
        self.assertIn("If the current door open like pattern persists", scenario.summary_headline)
        self.assertTrue(any("Worst forecast condition reaches" in line for line in scenario.summary_lines))
        self.assertTrue(any("Forecast source: event persist slope" in line for line in scenario.summary_lines))
    # Test purpose: Verifies that build forecast chart supports dew point metric
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_build_forecast_chart_supports_dew_point_metric(self) -> None:
        baseline_row = pd.Series(
            {
                "scenario": "baseline",
                "json_forecast": (
                    '{"temp_forecast_c":[24.2,24.3,24.4],'
                    '"rh_forecast_pct":[58.0,57.8,57.6],'
                    '"dew_point_forecast_c":[15.1,15.2,15.3],'
                    '"feature_vector":{"temp_last":24.0,"rh_last":58.2,"dew_last":15.0},'
                    '"source":"analogue_knn",'
                    '"neighbor_count":6,'
                    '"case_count":6,'
                    '"notes":"Median forecast over 6 nearest historical cases."}'
                ),
                "json_p25": '{"temp_c":[24.0,24.1,24.2],"rh_pct":[57.5,57.3,57.1]}',
                "json_p75": '{"temp_c":[24.4,24.5,24.6],"rh_pct":[58.5,58.3,58.1]}',
                "event_type": "none",
                "MAE_T": None,
                "RMSE_T": None,
                "MAE_RH": None,
                "RMSE_RH": None,
                "large_error": None,
                "evaluation_notes": "",
            }
        )
        baseline = _scenario_view(baseline_row)

        chart_html = _build_forecast_chart(
            ts_pc_utc=pd.Timestamp("2026-03-28T00:00:00Z").to_pydatetime(),
            baseline=baseline,
            alternate=None,
            metric="dew",
            display_timezone=timezone.utc,
        )

        self.assertIn("30-minute dew point forecast", chart_html)
        self.assertIn("Dew Point (C)", chart_html)
    # Test purpose: Verifies that metric summary cards are built per plot with
    #   event variant behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_metric_summary_cards_are_built_per_plot_with_event_variant(self) -> None:
        baseline_row = pd.Series(
            {
                "scenario": "baseline",
                "json_forecast": (
                    '{"temp_forecast_c":[24.4,24.6,24.9],'
                    '"rh_forecast_pct":[58.2,58.1,58.0],'
                    '"dew_point_forecast_c":[15.2,15.3,15.5],'
                    '"feature_vector":{"temp_last":24.0,"rh_last":58.2,"dew_last":15.0},'
                    '"source":"analogue_knn",'
                    '"neighbor_count":6,'
                    '"case_count":8,'
                    '"notes":"Median forecast over nearest historical cases."}'
                ),
                "json_p25": '{"temp_c":[24.1,24.3,24.5],"rh_pct":[57.6,57.5,57.4]}',
                "json_p75": '{"temp_c":[24.7,24.9,25.1],"rh_pct":[58.8,58.7,58.6]}',
                "event_type": "door_open_like",
                "MAE_T": None,
                "RMSE_T": None,
                "MAE_RH": None,
                "RMSE_RH": None,
                "large_error": None,
                "evaluation_notes": "",
            }
        )
        event_row = pd.Series(
            {
                "scenario": "event_persist",
                "json_forecast": (
                    '{"temp_forecast_c":[24.6,24.9,25.2],'
                    '"rh_forecast_pct":[58.6,59.1,59.6],'
                    '"dew_point_forecast_c":[15.4,15.8,16.2],'
                    '"feature_vector":{"temp_last":24.0,"rh_last":58.2,"dew_last":15.0},'
                    '"source":"event_persist_slope",'
                    '"neighbor_count":0,'
                    '"case_count":0,'
                    '"notes":"Continues the current event slope."}'
                ),
                "json_p25": '{"temp_c":[24.4,24.7,25.0],"rh_pct":[58.0,58.5,59.0]}',
                "json_p75": '{"temp_c":[24.8,25.1,25.4],"rh_pct":[59.2,59.7,60.2]}',
                "event_type": "door_open_like",
                "MAE_T": None,
                "RMSE_T": None,
                "MAE_RH": None,
                "RMSE_RH": None,
                "large_error": None,
                "evaluation_notes": "",
            }
        )
        baseline = _scenario_view(baseline_row)
        event = _scenario_view(event_row)

        cards = _build_metric_summary_cards(
            metric="dew",
            baseline=baseline,
            alternate=event,
            event_type="door_open_like",
        )

        self.assertEqual(len(cards), 2)
        self.assertEqual(cards[0].scenario_label, "Baseline")
        self.assertEqual(cards[1].scenario_label, "Event persist")
        self.assertIn("Dew point rises", cards[0].headline)
        self.assertIn("If the current door open like pattern persists", cards[1].headline)
        self.assertEqual(cards[0].current_value, "15.00 C")
        self.assertTrue(cards[1].peak_value.endswith("at +3 min"))
    # Test purpose: Verifies that event persist can be reconstructed from recent
    #   history when storage row is missing behaves as expected under this
    #   regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on PredictionServiceTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_event_persist_can_be_reconstructed_from_recent_history_when_storage_row_is_missing(self) -> None:
        baseline_row = pd.Series(
            {
                "scenario": "baseline",
                "json_forecast": (
                    '{"temp_forecast_c":[24.2,24.3,24.4],'
                    '"rh_forecast_pct":[58.0,57.8,57.6],'
                    '"dew_point_forecast_c":[15.1,15.2,15.3],'
                    '"feature_vector":{"temp_last":24.0,"rh_last":58.2,"dew_last":15.0},'
                    '"source":"analogue_knn",'
                    '"neighbor_count":6,'
                    '"case_count":6,'
                    '"notes":"Median forecast over 6 nearest historical cases."}'
                ),
                "json_p25": '{"temp_c":[24.0,24.1,24.2],"rh_pct":[57.5,57.3,57.1]}',
                "json_p75": '{"temp_c":[24.4,24.5,24.6],"rh_pct":[58.5,58.3,58.1]}',
                "event_type": "none",
                "MAE_T": None,
                "RMSE_T": None,
                "MAE_RH": None,
                "RMSE_RH": None,
                "large_error": None,
                "evaluation_notes": "",
            }
        )
        baseline = _scenario_view(baseline_row)
        history = pd.DataFrame(
            {
                "ts_pc_utc": pd.date_range("2026-03-28T00:00:00Z", periods=30, freq="min"),
                "temp_c": [24.0 + 0.04 * index for index in range(30)],
                "rh_pct": [58.2 + 0.12 * index for index in range(30)],
                "dew_point_c": [15.0 + 0.05 * index for index in range(30)],
            }
        )

        scenario = _event_persist_from_history_frame(
            history_frame=history,
            baseline=baseline,
            event_type="none",
        )

        self.assertIsNotNone(scenario)
        assert scenario is not None
        self.assertEqual(scenario.source, "event_persist_slope")
        self.assertEqual(scenario.scenario, "event_persist")
        self.assertIn("If the current live conditions persist", scenario.summary_headline)
        self.assertGreater(scenario.temp_end_c, scenario.temp_start_c)
        self.assertGreater(scenario.rh_end_pct, scenario.rh_start_pct)


if __name__ == "__main__":
    unittest.main()
