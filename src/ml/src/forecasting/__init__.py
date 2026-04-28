# File overview:
# - Responsibility: Forecasting package used by the warehouse monitoring prototype.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Forecasting package used by the warehouse monitoring prototype.

This module is a useful "map" file to show in a viva because it lists the major
stages of the forecasting pipeline in one place.

In project terms, the flow is:
1. raw telemetry is read and resampled by the gateway forecast adapter
2. recent-event detection decides whether conditions look normal or disturbed
3. the 3-hour history window is turned into a compact feature vector
4. an analogue / kNN forecaster produces the baseline 30-minute trajectory
5. an optional event-persist scenario is generated when recent behaviour looks
   disturbance-like
6. the forecast is evaluated later, stored, and then read by the dashboard

Keeping these imports in ``__all__`` makes the higher-level runner code easier
to read: the gateway can import the forecasting package almost as a small
domain-specific toolkit rather than a scattered set of utilities.
"""

from forecasting.case_base import CaseBaseStore
from forecasting.config import ForecastConfig, MODEL_VERSION, build_config
from forecasting.evaluator import evaluate_forecast
from forecasting.event_detection import detect_recent_event
from forecasting.features import extract_feature_vector
from forecasting.filtering import build_baseline_window
from forecasting.knn_forecaster import AnalogueKNNForecaster
from forecasting.scenario import build_event_persist_forecast

__all__ = [
    "AnalogueKNNForecaster",
    "CaseBaseStore",
    "ForecastConfig",
    "MODEL_VERSION",
    "build_baseline_window",
    "build_config",
    "build_event_persist_forecast",
    "detect_recent_event",
    "evaluate_forecast",
    "extract_feature_vector",
]
