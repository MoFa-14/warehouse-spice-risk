from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from app.services.timeseries_service import _build_metric_figure, _plotly_chart_config


class DashboardTimeseriesServiceTests(unittest.TestCase):
    def test_metric_figure_renders_gaps_as_dashed_bridge_segments(self) -> None:
        frame = pd.DataFrame(
            {
                "ts_pc_utc": [
                    pd.Timestamp(datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)),
                    pd.Timestamp(datetime(2026, 3, 29, 12, 1, tzinfo=timezone.utc)),
                    pd.Timestamp(datetime(2026, 3, 29, 12, 4, tzinfo=timezone.utc)),
                    pd.Timestamp(datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc)),
                ],
                "value": [20.0, 20.3, 20.7, 20.9],
            }
        )

        figure = _build_metric_figure(
            frame,
            title="Temperature vs Time",
            y_label="Temperature (C)",
            color="#d97706",
            max_points=500,
            display_timezone=timezone.utc,
        )

        self.assertIsNotNone(figure)
        self.assertEqual(len(figure.data), 2)
        observed_trace = figure.data[0]
        gap_trace = figure.data[1]
        self.assertEqual(observed_trace.mode, "lines+markers")
        self.assertIn(None, list(observed_trace.x))
        self.assertEqual(gap_trace.name, "No readings")
        self.assertEqual(gap_trace.line.dash, "dash")
        self.assertEqual(len([value for value in gap_trace.x if value is not None]), 2)
        self.assertEqual(gap_trace.hovertemplate, "No readings captured in this interval<extra></extra>")

    def test_plotly_chart_config_enables_zoom_controls(self) -> None:
        config = _plotly_chart_config()

        self.assertTrue(config["displayModeBar"])
        self.assertTrue(config["scrollZoom"])
        self.assertEqual(config["doubleClick"], "reset")
        self.assertIn("lasso2d", config["modeBarButtonsToRemove"])
        self.assertIn("select2d", config["modeBarButtonsToRemove"])

    def test_downsampling_does_not_create_fake_gap_trace_for_continuous_data(self) -> None:
        start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        frame = pd.DataFrame(
            {
                "ts_pc_utc": [pd.Timestamp(start + timedelta(minutes=index)) for index in range(12)],
                "value": [20.0 + 0.1 * index for index in range(12)],
            }
        )

        figure = _build_metric_figure(
            frame,
            title="Temperature vs Time",
            y_label="Temperature (C)",
            color="#d97706",
            max_points=4,
            display_timezone=timezone.utc,
        )

        self.assertIsNotNone(figure)
        self.assertEqual(len(figure.data), 1)


if __name__ == "__main__":
    unittest.main()
