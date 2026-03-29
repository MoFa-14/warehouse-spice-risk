"""Flask entrypoint for the Layer 4 dashboard."""

from __future__ import annotations

from pathlib import Path

from flask import Flask, abort, redirect, render_template, request, url_for

from app.config import DashboardConfig
from app.services import (
    acknowledge_alert,
    build_alert_snapshot,
    build_health_context,
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


def create_app(test_config: dict | None = None) -> Flask:
    """Create and configure the Flask dashboard application."""
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


def _ensure_runtime_paths(app: Flask) -> None:
    runtime_dir = Path(app.config["RUNTIME_DIR"])
    runtime_dir.mkdir(parents=True, exist_ok=True)
    ack_file = Path(app.config["ACKS_FILE"])
    if not ack_file.exists():
        ack_file.write_text("{}", encoding="utf-8")


def _register_filters(app: Flask) -> None:
    @app.template_filter("fmt_ts")
    def fmt_ts(value):
        return format_display_timestamp(value, _display_timezone(app))

    @app.template_filter("fmt_num")
    def fmt_num(value, digits: int = 2):
        if value is None:
            return "--"
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return "--"

    @app.template_filter("fmt_pct")
    def fmt_pct(value):
        if value is None:
            return "--"
        try:
            return f"{float(value) * 100:.2f}%"
        except (TypeError, ValueError):
            return "--"


def _register_context_processors(app: Flask) -> None:
    @app.context_processor
    def inject_dashboard_defaults():
        display_timezone = _display_timezone(app)
        return {
            "auto_refresh_seconds": int(app.config.get("AUTO_REFRESH_SECONDS", 0) or 0),
            "display_time_label": timezone_label(display_timezone),
        }


def _register_routes(app: Flask) -> None:
    @app.get("/")
    def index():
        readings, alert_snapshot = _base_context(app)
        return render_template(
            "index.html",
            page_title="Overview",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
        )

    @app.get("/pods/<pod_id>")
    def pod_detail(pod_id: str):
        readings, alert_snapshot = _base_context(app)
        display_timezone = _display_timezone(app)
        reading = get_latest_pod_reading(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            db_path=Path(app.config["DB_PATH"]),
        )
        if reading is None:
            abort(404)
        window = resolve_time_window(
            request.args.get("range"),
            request.args.get("start"),
            request.args.get("end"),
            display_timezone=display_timezone,
        )
        charts = build_timeseries_context(
            Path(app.config["DATA_ROOT"]),
            pod_id,
            window,
            max_points=int(app.config["MAX_CHART_POINTS"]),
            db_path=Path(app.config["DB_PATH"]),
            display_timezone=display_timezone,
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

    @app.get("/prediction")
    def prediction():
        readings, alert_snapshot = _base_context(app)
        context = build_prediction_page_context(
            Path(app.config["DATA_ROOT"]),
            db_path=Path(app.config["DB_PATH"]),
            selected_pod_id=request.args.get("pod"),
            display_timezone=_display_timezone(app),
        )
        return render_template(
            "prediction.html",
            page_title="Prediction",
            readings=readings,
            alert_banner=alert_snapshot["alert_banner"],
            prediction=context,
        )


def _base_context(app: Flask):
    readings = get_latest_pod_readings(
        Path(app.config["DATA_ROOT"]),
        db_path=Path(app.config["DB_PATH"]),
    )
    alert_snapshot = build_alert_snapshot(
        readings,
        Path(app.config["ACKS_FILE"]),
        ack_minutes=int(app.config["ACK_MINUTES"]),
    )
    return readings, alert_snapshot


def _display_timezone(app: Flask):
    return resolve_display_timezone(app.config.get("DISPLAY_TIMEZONE"))


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
