# File overview:
# - Responsibility: Flask entrypoint for the dashboard application.
# - Project role: Defines app configuration, initialization, route setup, and
#   dashboard-wide utilities.
# - Main data or concerns: Configuration values, route parameters, and app-level
#   helper state.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.
# - Why this matters: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.

"""Flask entrypoint for the dashboard application.

This file assembles the dashboard from its lower-level services. It does not
perform forecasting itself and it does not read raw CSV or SQLite tables
directly. Instead it wires routes to service-layer functions that prepare
dashboard-ready view models.
"""

from __future__ import annotations

from pathlib import Path

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

from app.config import DashboardConfig
from app.services import (
    acknowledge_alert,
    build_alert_snapshot,
    build_health_context,
    build_monitoring_review_context,
    build_pod_prediction_context,
    build_prediction_page_context,
    build_timeseries_context,
    get_latest_pod_reading,
    get_latest_pod_readings,
    resolve_time_window,
    threshold_legend,
)
from app.timezone import (
    format_datetime_local_value,
    format_display_timestamp,
    resolve_display_timezone,
    timezone_label,
)
# Function purpose: Create and configure the Flask application instance.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as test_config, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns Flask when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def create_app(test_config: dict | None = None) -> Flask:
    """Create and configure the Flask application instance.

    The route functions registered below are intentionally thin. Their job is to
    request the right service-layer context and then render the corresponding
    template. This keeps business logic out of the Flask entrypoint and makes
    the dashboard easier to test subsystem by subsystem.
    """
    app = Flask(
        __name__,
        template_folder="web/templates",
        static_folder="web/static",
    )
    app.config.from_object(DashboardConfig)
    if test_config:
        app.config.update(test_config)

    _ensure_runtime_paths(app)
    _register_filters(app)
    _register_context_processors(app)
    _register_routes(app)
    return app
# Function purpose: Create local runtime files expected by the dashboard.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _ensure_runtime_paths(app: Flask) -> None:
    """Create local runtime files expected by the dashboard."""
    runtime_dir = Path(app.config["RUNTIME_DIR"])
    runtime_dir.mkdir(parents=True, exist_ok=True)
    ack_file = Path(app.config["ACKS_FILE"])
    if not ack_file.exists():
        ack_file.write_text("{}", encoding="utf-8")
# Function purpose: Register presentation-only helpers used in Jinja templates.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _register_filters(app: Flask) -> None:
    """Register presentation-only helpers used in Jinja templates."""
    # Function purpose: Implements the fmt timestamp step used by this
    #   subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: Arguments such as value, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.template_filter("fmt_ts")
    def fmt_ts(value):
        return format_display_timestamp(value, _display_timezone(app))
    # Function purpose: Implements the fmt num step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: Arguments such as value, digits, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.template_filter("fmt_num")
    def fmt_num(value, digits: int = 2):
        if value is None:
            return "--"
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return "--"
    # Function purpose: Implements the fmt pct step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: Arguments such as value, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.template_filter("fmt_pct")
    def fmt_pct(value):
        if value is None:
            return "--"
        try:
            return f"{float(value) * 100:.2f}%"
        except (TypeError, ValueError):
            return "--"
# Function purpose: Implements the register context processors step used by this
#   subsystem.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _register_context_processors(app: Flask) -> None:
    # Function purpose: Implements the inject dashboard defaults step used by
    #   this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.context_processor
    def inject_dashboard_defaults():
        display_timezone = _display_timezone(app)
        return {
            "auto_refresh_seconds": int(app.config.get("AUTO_REFRESH_SECONDS", 0) or 0),
            "display_time_label": timezone_label(display_timezone),
        }
# Function purpose: Register the main dashboard pages and small JSON API endpoint.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _register_routes(app: Flask) -> None:
    """Register the main dashboard pages and small JSON API endpoint."""
    # Function purpose: Implements the index step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/")
    def index():
        readings, alert_snapshot = _base_context(app)
        return render_template(
            "index.html",
            page_title="Overview",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
        )
    # Function purpose: Render the per-pod page with latest state, history, and
    #   forecasts.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/pods/<pod_id>")
    def pod_detail(pod_id: str):
        """Render the per-pod page with latest state, history, and forecasts."""
        readings, alert_snapshot = _base_context(app)
        display_timezone = _display_timezone(app)
        reading = get_latest_pod_reading(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            db_path=Path(app.config["DB_PATH"]),
            adjustments_path=Path(app.config["TELEMETRY_ADJUSTMENTS_PATH"]),
        )
        if reading is None:
            abort(404)
        window = resolve_time_window(
            request.args.get("range"),
            request.args.get("start"),
            request.args.get("end"),
            display_timezone=display_timezone,
            reference_end=reading.ts_pc_utc,
        )
        charts = build_timeseries_context(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            window,
            max_points=int(app.config["MAX_CHART_POINTS"]),
            db_path=Path(app.config["DB_PATH"]),
            display_timezone=display_timezone,
            adjustments_path=Path(app.config["TELEMETRY_ADJUSTMENTS_PATH"]),
        )
        prediction = build_pod_prediction_context(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            db_path=Path(app.config["DB_PATH"]),
            display_timezone=display_timezone,
        )
        preset_ranges = [("1h", "1h"), ("6h", "6h"), ("24h", "24h"), ("7d", "7d")]
        return render_template(
            "pod_detail.html",
            page_title=f"Pod {pod_id}",
            reading=reading,
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            charts=charts,
            threshold_legend=threshold_legend(),
            preset_ranges=preset_ranges,
            custom_start=format_datetime_local_value(window.start, display_timezone),
            custom_end=format_datetime_local_value(window.end, display_timezone),
            prediction=prediction,
        )
    # Function purpose: Implements the health step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/health")
    def health():
        readings, alert_snapshot = _base_context(app)
        health_context = build_health_context(Path(app.config["DATA_ROOT"]), db_path=Path(app.config["DB_PATH"]))
        return render_template(
            "health.html",
            page_title="Health",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            health=health_context,
        )
    # Function purpose: Implements the alerts step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/alerts")
    def alerts():
        readings, alert_snapshot = _base_context(app)
        return render_template(
            "alerts.html",
            page_title="Alerts",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            active_alerts=alert_snapshot["active_alerts"],
            acknowledged_alerts=alert_snapshot["acknowledged_alerts"],
            ack_minutes=alert_snapshot["ack_minutes"],
        )
    # Function purpose: Implements the acknowledge step used by this subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.post("/alerts/acknowledge")
    def acknowledge():
        ack_key = request.form.get("ack_key", "").strip()
        if ack_key:
            acknowledge_alert(
                Path(app.config["ACKS_FILE"]),
                ack_key,
                minutes=int(app.config["ACK_MINUTES"]),
            )
        return redirect(request.form.get("next") or url_for("alerts"))
    # Function purpose: Render the forecasting-focused page built from persisted
    #   outputs.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/prediction")
    def prediction():
        """Render the forecasting-focused page built from persisted outputs."""
        readings, alert_snapshot = _base_context(app)
        context = build_prediction_page_context(
            Path(app.config["DATA_ROOT"]),
            db_path=Path(app.config["DB_PATH"]),
            selected_pod_id=request.args.get("pod"),
            selected_test_attempt_ts=request.args.get("test_attempt"),
            display_timezone=_display_timezone(app),
        )
        return render_template(
            "prediction.html",
            page_title="Prediction",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            prediction=context,
        )
    # Function purpose: Reviews review and returns the derived output expected
    #   by the caller.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/review")
    def review():
        readings, alert_snapshot = _base_context(app)
        display_timezone = _display_timezone(app)
        window = resolve_time_window(
            request.args.get("range") or "7d",
            request.args.get("start"),
            request.args.get("end"),
            display_timezone=display_timezone,
        )
        context = build_monitoring_review_context(
            Path(app.config["DATA_ROOT"]),
            window=window,
            db_path=Path(app.config["DB_PATH"]),
            pod_id=request.args.get("pod"),
            acks_file=Path(app.config["ACKS_FILE"]),
        )
        return render_template(
            "review.html",
            page_title="Review",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            review=context,
        )
    # Function purpose: Implements the api latest pod reading step used by this
    #   subsystem.
    # - Project role: Belongs to the dashboard application wiring layer and
    #   contributes one focused step within that subsystem.
    # - Inputs: Arguments such as pod_id, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns the computed value, structured record, or side effect
    #   defined by the implementation.
    # - Important decisions: App wiring needs to stay compact and explicit
    #   because every dashboard page depends on it.
    # - Related flow: Coordinates dashboard services, configuration, and Flask
    #   entry points.

    @app.get("/api/pods/<pod_id>/latest")
    def api_latest_pod_reading(pod_id: str):
        reading = get_latest_pod_reading(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            db_path=Path(app.config["DB_PATH"]),
            adjustments_path=Path(app.config["TELEMETRY_ADJUSTMENTS_PATH"]),
        )
        if reading is None:
            return jsonify({"error": "pod_not_found", "pod_id": pod_id}), 404
        return jsonify(
            {
                "pod_id": reading.pod_id,
                "ts_pc_utc": reading.ts_pc_utc.isoformat().replace("+00:00", "Z"),
                "temp_c": reading.temp_c,
                "rh_pct": reading.rh_pct,
                "dew_point_c": reading.dew_point_c,
                "data_source": reading.data_source,
                "has_measurement": reading.has_measurement,
                "last_complete_ts_pc_utc": None
                if reading.last_complete_ts_pc_utc is None
                else reading.last_complete_ts_pc_utc.isoformat().replace("+00:00", "Z"),
                "status": None if reading.status is None else reading.status.level_label,
            }
        )
# Function purpose: Build the common dashboard chrome shared by most pages.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _base_context(app: Flask):
    """Build the common dashboard chrome shared by most pages."""
    readings = get_latest_pod_readings(
        Path(app.config["DATA_ROOT"]),
        db_path=Path(app.config["DB_PATH"]),
        adjustments_path=Path(app.config["TELEMETRY_ADJUSTMENTS_PATH"]),
    )
    alert_snapshot = build_alert_snapshot(
        readings,
        Path(app.config["ACKS_FILE"]),
        ack_minutes=int(app.config["ACK_MINUTES"]),
    )
    return readings, alert_snapshot
# Function purpose: Implements the display timezone step used by this subsystem.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as app, interpreted according to the rules encoded in the
#   body below.
# - Outputs: Returns the computed value, structured record, or side effect defined
#   by the implementation.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _display_timezone(app: Flask):
    return resolve_display_timezone(app.config.get("DISPLAY_TIMEZONE"))


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
