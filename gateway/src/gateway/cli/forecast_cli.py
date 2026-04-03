"""Forecast CLI for one-shot and recurring pod forecasting."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Sequence

from gateway.forecast import _ensure_forecasting_package
from gateway.forecast.runner import ForecastRunner

_ensure_forecasting_package()

from forecasting.scheduler import ForecastScheduler


LOGGER = logging.getLogger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warehouse Spice Risk forecasting tools")
    subparsers = parser.add_subparsers(dest="command", required=True)

    once = _add_common_arguments(subparsers.add_parser("once", help="Run one forecasting pass for one or more pods."))
    run = _add_common_arguments(subparsers.add_parser("run", help="Run recurring forecasting on a fixed cadence."))
    run.add_argument("--every-minutes", type=int, default=30, help="Forecast cadence in minutes. Defaults to 30.")
    run.add_argument("--duration", type=float, help="Optional total runtime in seconds.")

    args = parser.parse_args(argv)
    _validate_args(parser, args)
    return args


def _add_common_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--pod", action="append", help="Pod id to forecast. Repeat for multiple pods.")
    scope.add_argument("--all", action="store_true", help="Forecast every known pod.")
    parser.add_argument("--k", type=int, default=10, help="Number of nearest analogue cases. Defaults to 10.")
    parser.add_argument(
        "--history-minutes",
        type=int,
        default=180,
        help="Forecast history window. This forecaster currently only supports 180 minutes.",
    )
    parser.add_argument("--horizon-minutes", type=int, default=30, help="Forecast horizon in minutes. Defaults to 30.")
    parser.add_argument("--missing-rate-max", type=float, default=0.10, help="Maximum acceptable missing-rate for case updates.")
    parser.add_argument("--storage", choices=("sqlite", "csv"), default="sqlite", help="Telemetry source backend.")
    parser.add_argument("--db-path", default="data/db/telemetry.sqlite", help="SQLite database path.")
    parser.add_argument(
        "--telemetry-adjustments",
        help="Optional JSON file with per-pod calibration and forecast smoothing settings.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable more detailed logging.")
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.history_minutes != 180:
        parser.error("--history-minutes is fixed to 180 for this task.")
    if args.horizon_minutes <= 0:
        parser.error("--horizon-minutes must be positive.")
    if args.k <= 0:
        parser.error("--k must be positive.")
    if args.missing_rate_max < 0.0 or args.missing_rate_max > 1.0:
        parser.error("--missing-rate-max must be between 0 and 1.")
    if getattr(args, "every_minutes", 30) <= 0:
        parser.error("--every-minutes must be positive.")
    if getattr(args, "duration", None) is not None and args.duration <= 0:
        parser.error("--duration must be positive when provided.")


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("gateway").setLevel(logging.DEBUG if verbose else logging.INFO)


def cli(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    runner = ForecastRunner(
        storage_backend=args.storage,
        db_path=args.db_path,
        adjustments_path=args.telemetry_adjustments,
        k=args.k,
        history_minutes=args.history_minutes,
        horizon_minutes=args.horizon_minutes,
        missing_rate_max=args.missing_rate_max,
    )
    pod_ids = runner.pod_ids(requested_pods=args.pod, use_all=args.all)
    if not pod_ids:
        LOGGER.error("No pods found for forecasting.")
        return 1

    if args.command == "once":
        bundles = runner.run_cycle(pod_ids=pod_ids)
        print(json.dumps([_bundle_to_dict(bundle) for bundle in bundles], indent=2))
        return 0

    scheduler = ForecastScheduler(every_minutes=args.every_minutes, duration_s=args.duration, align_to_wall_clock=False)

    def _callback(run_time: datetime) -> None:
        LOGGER.info("forecast cycle start ts=%s pods=%s backend=%s", run_time.isoformat(), ",".join(pod_ids), runner.active_storage_backend)
        runner.run_cycle(pod_ids=pod_ids, requested_time_utc=run_time.astimezone(timezone.utc))

    scheduler.run(_callback)
    return 0


def _bundle_to_dict(bundle) -> dict[str, object]:
    payload = {
        "pod_id": bundle.pod_id,
        "ts_pc_utc": bundle.ts_pc_utc,
        "model_version": bundle.model_version,
        "missing_rate": bundle.missing_rate,
        "event_detected": bundle.event.event_detected,
        "event_type": bundle.event.event_type,
        "event_reason": bundle.event.event_reason,
        "baseline": _scenario_to_dict(bundle.baseline),
    }
    if bundle.event_persist is not None:
        payload["event_persist"] = _scenario_to_dict(bundle.event_persist)
    return payload


def _scenario_to_dict(trajectory) -> dict[str, object]:
    return {
        "scenario": trajectory.scenario,
        "source": trajectory.source,
        "neighbor_count": trajectory.neighbor_count,
        "case_count": trajectory.case_count,
        "temp_forecast_c": trajectory.temp_forecast_c,
        "rh_forecast_pct": trajectory.rh_forecast_pct,
        "dew_point_forecast_c": trajectory.dew_point_forecast_c,
        "temp_p25_c": trajectory.temp_p25_c,
        "temp_p75_c": trajectory.temp_p75_c,
        "rh_p25_pct": trajectory.rh_p25_pct,
        "rh_p75_pct": trajectory.rh_p75_pct,
        "notes": trajectory.notes,
    }


if __name__ == "__main__":
    raise SystemExit(cli())
