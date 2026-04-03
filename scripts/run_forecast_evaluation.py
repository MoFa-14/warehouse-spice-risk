"""Run a leakage-safe historical forecast evaluation for the dissertation."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
for package_root in (ROOT / "src" / "gateway" / "src", ROOT / "src" / "ml" / "src"):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from gateway.forecast.storage_adapter import ForecastStorageAdapter
from forecasting import AnalogueKNNForecaster, build_baseline_window, build_config, detect_recent_event, extract_feature_vector
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import CaseRecord, FeatureVector, TimeSeriesPoint
from forecasting.utils import floor_to_interval, parse_utc, to_utc_iso


UTC = timezone.utc
DEFAULT_DB_PATH = ROOT / "src" / "data" / "db" / "telemetry.sqlite"
DEFAULT_RESULTS_DIR = ROOT / "evaluation" / "results"


@dataclass(frozen=True)
class CoverageDay:
    day: str
    shared_minutes: int
    shared_start_utc: str
    shared_end_utc: str
    shared_span_hours: float
    pod_sample_counts: dict[str, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run forecast metrics and persistence baseline evaluation.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="SQLite database to evaluate.")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR, help="Directory for CSV/JSON outputs.")
    parser.add_argument("--day", help="Optional UTC date override in YYYY-MM-DD form. Defaults to the best shared two-pod day.")
    parser.add_argument("--step-minutes", type=int, default=10, help="Cadence between forecast windows. Defaults to 10.")
    parser.add_argument("--history-minutes", type=int, default=180, help="Forecast history window. Must remain 180.")
    parser.add_argument("--horizon-minutes", type=int, default=30, help="Forecast horizon. Defaults to 30.")
    parser.add_argument("--k", type=int, default=10, help="Nearest-neighbour count for the analogue forecaster.")
    parser.add_argument(
        "--seed-step-minutes",
        type=int,
        default=30,
        help="Cadence used to build earlier no-leakage analogue cases before the selected day.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.history_minutes != 180:
        raise SystemExit("--history-minutes must remain 180 to match the implemented forecaster.")
    if args.horizon_minutes != 30:
        raise SystemExit("--horizon-minutes must remain 30 to match the implemented forecaster.")
    if args.step_minutes <= 0 or args.seed_step_minutes <= 0:
        raise SystemExit("--step-minutes and --seed-step-minutes must be positive.")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    coverage_days = _shared_coverage_days(args.db_path)
    if not coverage_days:
        raise SystemExit("No overlapping pod days were found in the SQLite database.")

    selected_day = _select_day(coverage_days, args.day)
    config = build_config(k=args.k, history_minutes=args.history_minutes, horizon_minutes=args.horizon_minutes)
    adapter = ForecastStorageAdapter(storage_backend="sqlite", db_path=args.db_path)
    knn = AnalogueKNNForecaster(config=config)

    seed_cutoff = datetime.combine(date.fromisoformat(selected_day.day), dt_time.min, tzinfo=UTC)
    seeded_cases = _seed_cases(
        adapter=adapter,
        db_path=args.db_path,
        seed_cutoff_utc=seed_cutoff,
        config=config,
        step_minutes=args.seed_step_minutes,
    )

    window_records: list[dict[str, object]] = []
    comparison_records: list[dict[str, object]] = []
    implemented_buckets = _bucket_map()
    persistence_buckets = _bucket_map()
    source_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    forecast_times = list(
        _candidate_times(
            selected_day=selected_day,
            history_minutes=config.history_minutes,
            horizon_minutes=config.horizon_minutes,
            step_minutes=args.step_minutes,
        )
    )

    sequential_cases = {pod_id: list(cases) for pod_id, cases in seeded_cases.items()}
    for forecast_time in forecast_times:
        for pod_id in ("01", "02"):
            history = adapter.load_history_window(pod_id=pod_id, as_of_utc=forecast_time, minutes=config.history_minutes)
            actual = adapter.load_actual_horizon(pod_id=pod_id, ts_forecast_utc=forecast_time, minutes=config.horizon_minutes)
            if len(history.points) < config.history_minutes or len(actual.points) < config.horizon_minutes:
                continue

            event = detect_recent_event(history.points, config=config)
            category = "disturbed" if event.event_detected else "stable"
            baseline_window = build_baseline_window(history.points, detection=event, config=config)
            feature_vector = extract_feature_vector(baseline_window)
            usable_cases = _usable_cases_before(sequential_cases[pod_id], forecast_time)
            trajectory = knn.forecast(feature_vector=feature_vector, baseline_window=baseline_window, cases=usable_cases)
            persistence_trajectory = _build_persistence_trajectory(history.points, config.horizon_minutes)

            actual_temp = [point.temp_c for point in actual.points[: config.horizon_minutes]]
            actual_rh = [point.rh_pct for point in actual.points[: config.horizon_minutes]]
            actual_dew = [point.dew_point_c for point in actual.points[: config.horizon_minutes]]

            impl_metrics = _series_metrics(
                trajectory.temp_forecast_c,
                actual_temp,
                trajectory.rh_forecast_pct,
                actual_rh,
                trajectory.dew_point_forecast_c,
                actual_dew,
            )
            persistence_metrics = _series_metrics(
                persistence_trajectory["temp_forecast_c"],
                actual_temp,
                persistence_trajectory["rh_forecast_pct"],
                actual_rh,
                persistence_trajectory["dew_point_forecast_c"],
                actual_dew,
            )

            source_counts[category][trajectory.source] += 1
            source_counts["overall"][trajectory.source] += 1
            _update_bucket(implemented_buckets[category], impl_metrics)
            _update_bucket(implemented_buckets["overall"], impl_metrics)
            _update_bucket(implemented_buckets[f"pod:{pod_id}"], impl_metrics)
            _update_bucket(persistence_buckets[category], persistence_metrics)
            _update_bucket(persistence_buckets["overall"], persistence_metrics)
            _update_bucket(persistence_buckets[f"pod:{pod_id}"], persistence_metrics)

            record = {
                "day": selected_day.day,
                "forecast_time_utc": to_utc_iso(forecast_time),
                "pod_id": pod_id,
                "category": category,
                "event_type": event.event_type,
                "event_reason": event.event_reason,
                "history_missing_rate": history.missing_rate,
                "actual_missing_rate": actual.missing_rate,
                "case_count_before": len(usable_cases),
                "model_source": trajectory.source,
                "neighbor_count": trajectory.neighbor_count,
                "temp_mae": impl_metrics["temp_mae"],
                "temp_rmse": impl_metrics["temp_rmse"],
                "rh_mae": impl_metrics["rh_mae"],
                "rh_rmse": impl_metrics["rh_rmse"],
                "dew_mae": impl_metrics["dew_mae"],
                "dew_rmse": impl_metrics["dew_rmse"],
            }
            comparison = {
                "day": selected_day.day,
                "forecast_time_utc": to_utc_iso(forecast_time),
                "pod_id": pod_id,
                "category": category,
                "event_type": event.event_type,
                "history_missing_rate": history.missing_rate,
                "actual_missing_rate": actual.missing_rate,
                "implemented_source": trajectory.source,
                "implemented_case_count_before": len(usable_cases),
                "implemented_neighbor_count": trajectory.neighbor_count,
                "implemented_temp_mae": impl_metrics["temp_mae"],
                "implemented_temp_rmse": impl_metrics["temp_rmse"],
                "implemented_rh_mae": impl_metrics["rh_mae"],
                "implemented_rh_rmse": impl_metrics["rh_rmse"],
                "implemented_dew_mae": impl_metrics["dew_mae"],
                "implemented_dew_rmse": impl_metrics["dew_rmse"],
                "persistence_temp_mae": persistence_metrics["temp_mae"],
                "persistence_temp_rmse": persistence_metrics["temp_rmse"],
                "persistence_rh_mae": persistence_metrics["rh_mae"],
                "persistence_rh_rmse": persistence_metrics["rh_rmse"],
                "persistence_dew_mae": persistence_metrics["dew_mae"],
                "persistence_dew_rmse": persistence_metrics["dew_rmse"],
            }
            window_records.append(record)
            comparison_records.append(comparison)

            if history.missing_rate <= config.missing_rate_max and actual.missing_rate <= config.missing_rate_max:
                sequential_cases[pod_id].append(
                    CaseRecord(
                        ts_pc_utc=feature_vector.ts_pc_utc,
                        pod_id=pod_id,
                        feature_vector=feature_vector.values,
                        future_temp_c=actual_temp,
                        future_rh_pct=actual_rh,
                        event_label=event.event_type if event.event_detected else "none",
                    )
                )

    implemented_summary = _summaries_from_buckets(implemented_buckets)
    persistence_summary = _summaries_from_buckets(persistence_buckets)
    comparison_summary = _comparison_summary(implemented_summary, persistence_summary)

    forecast_csv = args.results_dir / "forecast_metrics.csv"
    forecast_json = args.results_dir / "forecast_metrics.json"
    baseline_csv = args.results_dir / "baseline_comparison.csv"
    baseline_json = args.results_dir / "baseline_comparison.json"

    _write_csv(forecast_csv, window_records)
    _write_csv(baseline_csv, comparison_records)
    forecast_payload = {
        "selected_day": selected_day.__dict__,
        "history_minutes": config.history_minutes,
        "horizon_minutes": config.horizon_minutes,
        "step_minutes": args.step_minutes,
        "seed_step_minutes": args.seed_step_minutes,
        "seeded_case_counts": {pod_id: len(cases) for pod_id, cases in seeded_cases.items()},
        "evaluated_windows": len(window_records),
        "scenario_window_counts": {
            "stable": implemented_buckets["stable"]["windows"],
            "disturbed": implemented_buckets["disturbed"]["windows"],
            "overall": implemented_buckets["overall"]["windows"],
        },
        "source_counts": {key: dict(value) for key, value in source_counts.items()},
        "summary": implemented_summary,
        "per_pod_summary": {
            pod_id: implemented_summary[f"pod:{pod_id}"]
            for pod_id in ("01", "02")
            if f"pod:{pod_id}" in implemented_summary
        },
        "window_records_path": str(forecast_csv),
    }
    baseline_payload = {
        "selected_day": selected_day.__dict__,
        "history_minutes": config.history_minutes,
        "horizon_minutes": config.horizon_minutes,
        "step_minutes": args.step_minutes,
        "comparison": comparison_summary,
        "window_records_path": str(baseline_csv),
    }
    forecast_json.write_text(json.dumps(forecast_payload, indent=2), encoding="utf-8")
    baseline_json.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")

    print(json.dumps({"forecast_metrics": forecast_payload["summary"], "baseline_comparison": comparison_summary}, indent=2))
    return 0


def _shared_coverage_days(db_path: Path) -> list[CoverageDay]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT pod_id, ts_pc_utc
        FROM samples_raw
        WHERE pod_id IN ('01', '02')
        ORDER BY ts_pc_utc ASC
        """
    ).fetchall()
    connection.close()

    by_day: dict[str, dict[str, list[datetime]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        ts_value = parse_utc(row["ts_pc_utc"]).replace(second=0, microsecond=0)
        by_day[ts_value.date().isoformat()][str(row["pod_id"])].append(ts_value)

    coverage_days: list[CoverageDay] = []
    for day in sorted(by_day):
        if "01" not in by_day[day] or "02" not in by_day[day]:
            continue
        shared_minutes = sorted(set(by_day[day]["01"]) & set(by_day[day]["02"]))
        if not shared_minutes:
            continue
        span_hours = 0.0 if len(shared_minutes) == 1 else (shared_minutes[-1] - shared_minutes[0]).total_seconds() / 3600.0
        coverage_days.append(
            CoverageDay(
                day=day,
                shared_minutes=len(shared_minutes),
                shared_start_utc=to_utc_iso(shared_minutes[0]),
                shared_end_utc=to_utc_iso(shared_minutes[-1]),
                shared_span_hours=span_hours,
                pod_sample_counts={pod_id: len(values) for pod_id, values in by_day[day].items()},
            )
        )
    return coverage_days


def _select_day(days: list[CoverageDay], requested_day: str | None) -> CoverageDay:
    if requested_day is None:
        return max(days, key=lambda item: (item.shared_minutes, item.shared_span_hours, item.day))
    for item in days:
        if item.day == requested_day:
            return item
    raise SystemExit(f"Requested day {requested_day!r} is not available with shared two-pod coverage.")


def _seed_cases(
    *,
    adapter: ForecastStorageAdapter,
    db_path: Path,
    seed_cutoff_utc: datetime,
    config,
    step_minutes: int,
) -> dict[str, list[CaseRecord]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT pod_id, MIN(ts_pc_utc) AS min_ts
        FROM samples_raw
        WHERE pod_id IN ('01', '02')
        GROUP BY pod_id
        """
    ).fetchall()
    connection.close()

    result: dict[str, list[CaseRecord]] = {"01": [], "02": []}
    for row in rows:
        pod_id = str(row["pod_id"])
        earliest = parse_utc(row["min_ts"])
        start = earliest + timedelta(minutes=config.history_minutes)
        end = seed_cutoff_utc - timedelta(minutes=config.horizon_minutes)
        current = floor_to_interval(start, step_minutes)
        if current < start:
            current += timedelta(minutes=step_minutes)
        while current <= end:
            history = adapter.load_history_window(pod_id=pod_id, as_of_utc=current, minutes=config.history_minutes)
            actual = adapter.load_actual_horizon(pod_id=pod_id, ts_forecast_utc=current, minutes=config.horizon_minutes)
            if len(history.points) < config.history_minutes or len(actual.points) < config.horizon_minutes:
                current += timedelta(minutes=step_minutes)
                continue
            if history.missing_rate > config.missing_rate_max or actual.missing_rate > config.missing_rate_max:
                current += timedelta(minutes=step_minutes)
                continue
            event = detect_recent_event(history.points, config=config)
            feature_vector = extract_feature_vector(build_baseline_window(history.points, detection=event, config=config))
            result[pod_id].append(
                CaseRecord(
                    ts_pc_utc=feature_vector.ts_pc_utc,
                    pod_id=pod_id,
                    feature_vector=feature_vector.values,
                    future_temp_c=[point.temp_c for point in actual.points[: config.horizon_minutes]],
                    future_rh_pct=[point.rh_pct for point in actual.points[: config.horizon_minutes]],
                    event_label=event.event_type if event.event_detected else "none",
                )
            )
            current += timedelta(minutes=step_minutes)
    return result


def _candidate_times(
    *,
    selected_day: CoverageDay,
    history_minutes: int,
    horizon_minutes: int,
    step_minutes: int,
) -> Iterable[datetime]:
    start = parse_utc(selected_day.shared_start_utc) + timedelta(minutes=history_minutes)
    end = parse_utc(selected_day.shared_end_utc) - timedelta(minutes=horizon_minutes)
    current = floor_to_interval(start, step_minutes)
    if current < start:
        current += timedelta(minutes=step_minutes)
    while current <= end:
        yield current
        current += timedelta(minutes=step_minutes)


def _usable_cases_before(cases: list[CaseRecord], forecast_time: datetime) -> list[CaseRecord]:
    cutoff = to_utc_iso(forecast_time)
    return [
        case
        for case in cases
        if case.ts_pc_utc < cutoff and (case.event_label or "none") in {"", "none"}
    ]


def _build_persistence_trajectory(history_points: list[TimeSeriesPoint], horizon_minutes: int) -> dict[str, list[float]]:
    observed_points = [point for point in history_points if point.observed]
    latest = observed_points[-1] if observed_points else history_points[-1]
    temp_series = [latest.temp_c for _ in range(horizon_minutes)]
    rh_series = [latest.rh_pct for _ in range(horizon_minutes)]
    dew_series = [calculate_dew_point_c(latest.temp_c, latest.rh_pct) for _ in range(horizon_minutes)]
    return {
        "temp_forecast_c": temp_series,
        "rh_forecast_pct": rh_series,
        "dew_point_forecast_c": dew_series,
    }


def _series_metrics(
    predicted_temp: list[float],
    actual_temp: list[float],
    predicted_rh: list[float],
    actual_rh: list[float],
    predicted_dew: list[float],
    actual_dew: list[float],
) -> dict[str, object]:
    temp_abs = [abs(float(pred) - float(actual)) for pred, actual in zip(predicted_temp, actual_temp)]
    temp_sq = [(float(pred) - float(actual)) ** 2 for pred, actual in zip(predicted_temp, actual_temp)]
    rh_abs = [abs(float(pred) - float(actual)) for pred, actual in zip(predicted_rh, actual_rh)]
    rh_sq = [(float(pred) - float(actual)) ** 2 for pred, actual in zip(predicted_rh, actual_rh)]
    dew_abs = [abs(float(pred) - float(actual)) for pred, actual in zip(predicted_dew, actual_dew)]
    dew_sq = [(float(pred) - float(actual)) ** 2 for pred, actual in zip(predicted_dew, actual_dew)]
    return {
        "temp_abs_errors": temp_abs,
        "temp_sq_errors": temp_sq,
        "rh_abs_errors": rh_abs,
        "rh_sq_errors": rh_sq,
        "dew_abs_errors": dew_abs,
        "dew_sq_errors": dew_sq,
        "temp_mae": _mae_from_abs(temp_abs),
        "temp_rmse": _rmse_from_sq(temp_sq),
        "rh_mae": _mae_from_abs(rh_abs),
        "rh_rmse": _rmse_from_sq(rh_sq),
        "dew_mae": _mae_from_abs(dew_abs),
        "dew_rmse": _rmse_from_sq(dew_sq),
        "point_count": len(temp_abs),
    }


def _bucket_map() -> dict[str, dict[str, object]]:
    return defaultdict(
        lambda: {
            "windows": 0,
            "points": 0,
            "temp_abs_sum": 0.0,
            "temp_sq_sum": 0.0,
            "rh_abs_sum": 0.0,
            "rh_sq_sum": 0.0,
            "dew_abs_sum": 0.0,
            "dew_sq_sum": 0.0,
        }
    )


def _update_bucket(bucket: dict[str, object], metrics: dict[str, object]) -> None:
    bucket["windows"] += 1
    bucket["points"] += int(metrics["point_count"])
    bucket["temp_abs_sum"] += sum(metrics["temp_abs_errors"])
    bucket["temp_sq_sum"] += sum(metrics["temp_sq_errors"])
    bucket["rh_abs_sum"] += sum(metrics["rh_abs_errors"])
    bucket["rh_sq_sum"] += sum(metrics["rh_sq_errors"])
    bucket["dew_abs_sum"] += sum(metrics["dew_abs_errors"])
    bucket["dew_sq_sum"] += sum(metrics["dew_sq_errors"])


def _summaries_from_buckets(buckets: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
    return {key: _summarize_bucket(bucket) for key, bucket in buckets.items() if bucket["windows"] > 0}


def _summarize_bucket(bucket: dict[str, object]) -> dict[str, object]:
    points = int(bucket["points"])
    return {
        "windows": int(bucket["windows"]),
        "points": points,
        "temp_mae": 0.0 if points == 0 else float(bucket["temp_abs_sum"]) / points,
        "temp_rmse": 0.0 if points == 0 else math.sqrt(float(bucket["temp_sq_sum"]) / points),
        "rh_mae": 0.0 if points == 0 else float(bucket["rh_abs_sum"]) / points,
        "rh_rmse": 0.0 if points == 0 else math.sqrt(float(bucket["rh_sq_sum"]) / points),
        "dew_mae": 0.0 if points == 0 else float(bucket["dew_abs_sum"]) / points,
        "dew_rmse": 0.0 if points == 0 else math.sqrt(float(bucket["dew_sq_sum"]) / points),
    }


def _comparison_summary(
    implemented_summary: dict[str, dict[str, object]],
    persistence_summary: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for key in ("stable", "disturbed", "overall"):
        if key not in implemented_summary or key not in persistence_summary:
            continue
        implemented = implemented_summary[key]
        persistence = persistence_summary[key]
        rows[key] = {
            "implemented": implemented,
            "persistence": persistence,
            "better_than_persistence": _better_than_persistence(implemented, persistence),
            "percent_improvement": {
                "temp_mae": _percent_improvement(persistence["temp_mae"], implemented["temp_mae"]),
                "temp_rmse": _percent_improvement(persistence["temp_rmse"], implemented["temp_rmse"]),
                "rh_mae": _percent_improvement(persistence["rh_mae"], implemented["rh_mae"]),
                "rh_rmse": _percent_improvement(persistence["rh_rmse"], implemented["rh_rmse"]),
            },
        }
    return rows


def _better_than_persistence(implemented: dict[str, object], persistence: dict[str, object]) -> str:
    metrics = ("temp_mae", "temp_rmse", "rh_mae", "rh_rmse")
    better = [float(implemented[name]) < float(persistence[name]) for name in metrics]
    if all(better):
        return "Yes"
    if not any(better):
        return "No"
    return "Mixed"


def _percent_improvement(baseline: object, candidate: object) -> float | None:
    baseline_value = float(baseline)
    candidate_value = float(candidate)
    if baseline_value == 0.0:
        return None
    return ((baseline_value - candidate_value) / baseline_value) * 100.0


def _mae_from_abs(values: list[float]) -> float:
    return 0.0 if not values else sum(values) / float(len(values))


def _rmse_from_sq(values: list[float]) -> float:
    return 0.0 if not values else math.sqrt(sum(values) / float(len(values)))


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    raise SystemExit(main())
