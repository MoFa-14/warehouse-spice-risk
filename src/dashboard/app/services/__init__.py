# File overview:
# - Responsibility: Service exports for the dashboard.
# - Project role: Builds route-ready view models, chart inputs, and interpretive
#   summaries from loaded data.
# - Main data or concerns: View models, chart series, classifications, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.
# - Why this matters: Keeping presentation logic here prevents routes and templates
#   from reimplementing analysis rules.

"""Service exports for the dashboard."""

from app.services.alerts_service import acknowledge_alert, build_alert_snapshot
from app.services.link_service import build_health_context
from app.services.pod_service import PodLatestReading, discover_dashboard_pods, get_latest_pod_reading, get_latest_pod_readings
from app.services.prediction_service import build_pod_prediction_context, build_prediction_page_context
from app.services.review_service import build_monitoring_review_context
from app.services.thresholds import (
    RH_HIGH_RISK,
    RH_IDEAL_MAX,
    RH_IDEAL_MIN,
    RH_LOW,
    RH_MOLD_CRIT,
    RH_WARN_HIGH,
    T_CRIT_HIGH,
    T_OPT_MAX,
    T_OPT_MIN,
    T_WARN_HIGH,
    LEVEL_DEFINITIONS,
    classify_storage_trajectory,
    classify_storage_conditions,
    threshold_legend,
)
from app.services.timeseries_service import RANGE_OPTIONS, build_timeseries_context, resolve_time_window

__all__ = [
    "LEVEL_DEFINITIONS",
    "PodLatestReading",
    "RANGE_OPTIONS",
    "RH_HIGH_RISK",
    "RH_IDEAL_MAX",
    "RH_IDEAL_MIN",
    "RH_LOW",
    "RH_MOLD_CRIT",
    "RH_WARN_HIGH",
    "T_CRIT_HIGH",
    "T_OPT_MAX",
    "T_OPT_MIN",
    "T_WARN_HIGH",
    "acknowledge_alert",
    "build_alert_snapshot",
    "build_health_context",
    "build_monitoring_review_context",
    "build_pod_prediction_context",
    "build_prediction_page_context",
    "build_timeseries_context",
    "classify_storage_conditions",
    "classify_storage_trajectory",
    "discover_dashboard_pods",
    "get_latest_pod_reading",
    "get_latest_pod_readings",
    "resolve_time_window",
    "threshold_legend",
]
