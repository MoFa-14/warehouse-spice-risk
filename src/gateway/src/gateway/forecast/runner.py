"""Forecasting pipeline runner that connects gateway storage to the ML package.

This file is the operational heart of the forecasting subsystem. If the ML
package explains *how* a forecast is generated, this runner explains *when* it
is generated, *what data* it uses, and *where the result goes next*.

In end-to-end project terms, the runner:
- reads the latest telemetry for each pod
- prepares a 3-hour history window on a 1-minute grid
- asks the ML package for baseline and event-aware scenarios
- stores those forecasts for the dashboard
- later evaluates them once the real 30-minute future becomes available
- learns new analogue cases from completed windows
- applies light auto-calibration from recent trustworthy evaluations
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from gateway.forecast import _ensure_forecasting_package
from gateway.forecast.outputs import ForecastOutputs
from gateway.forecast.storage_adapter import ForecastStorageAdapter
from gateway.storage.paths import build_storage_paths

_ensure_forecasting_package()

from forecasting import (
    AnalogueKNNForecaster,
    CaseBaseStore,
    MODEL_VERSION,
    build_baseline_window,
    build_config,
    build_event_persist_forecast,
    detect_recent_event,
    evaluate_forecast,
    extract_feature_vector,
)
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import CaseRecord, ForecastBundle, EvaluationMetrics
from forecasting.utils import clamp, floor_to_interval, parse_utc, to_utc_iso


LOGGER = logging.getLogger(__name__)
AUTO_CASE_STEP_MINUTES = 30
CALIBRATION_WINDOW = 12
CALIBRATION_TEMP_CAP_C = 1.5
CALIBRATION_RH_CAP_PCT = 8.0


class ForecastRunner:
    """Run one-shot or continuous per-pod forecasting cycles.

    This class is what the CLI and helper scripts call. It is therefore the
    cleanest place to show a supervisor how the forecasting subsystem behaves as
    a complete service rather than as isolated ML functions.
    """

    def __init__(
        self,
        *,
        storage_backend: str,
        db_path=None,
        data_root=None,
        adjustments_path=None,
        k: int | None = None,
        history_minutes: int = 180,
        horizon_minutes: int = 30,
        missing_rate_max: float | None = None,
    ) -> None:
        """Initialise the runner and connect all forecasting subsystems.

        The constructor wires together three layers:
        - configuration: fixed forecasting assumptions
        - storage access: where telemetry, forecasts, and evaluations live
        - modelling tools: analogue forecaster and case base
        """
        self.config = build_config(
            k=k,
            missing_rate_max=missing_rate_max,
            history_minutes=history_minutes,
            horizon_minutes=horizon_minutes,
        )
        self.adapter = ForecastStorageAdapter(
            storage_backend=storage_backend,
            db_path=db_path,
            data_root=data_root,
            adjustments_path=adjustments_path,
        )
        self.outputs = ForecastOutputs(storage_backend=self.adapter.storage_backend, db_path=db_path, data_root=data_root)
        ml_root = build_storage_paths(data_root).root / "ml"
        self.case_base = CaseBaseStore(
            storage_backend=self.adapter.storage_backend,
            sqlite_db_path=self.outputs.db_path,
            jsonl_path=ml_root / "case_base.jsonl",
        )
        self.knn = AnalogueKNNForecaster(config=self.config)
        self._buffers: dict[str, list] = {}
        self.learning_interval_minutes = max(self.config.horizon_minutes, AUTO_CASE_STEP_MINUTES)
        self.calibration_window = CALIBRATION_WINDOW
        self.outputs.ensure_storage()
        self.case_base.ensure_storage()

    @property
    def active_storage_backend(self) -> str:
        return self.adapter.storage_backend

    @property
    def lock_target(self) -> Path:
        if self.adapter.storage_backend == "sqlite":
            return self.outputs.db_path.parent / "forecast_runner.sqlite"
        return self.outputs.forecasts_jsonl.parent / "forecast_runner.sqlite"

    def pod_ids(self, requested_pods: list[str] | None = None, use_all: bool = False) -> list[str]:
        """Resolve the list of pods the current run should operate on."""
        if use_all:
            return self.adapter.list_pod_ids()
        return sorted(set(requested_pods or []))

    def run_cycle(self, *, pod_ids: list[str], requested_time_utc: datetime | None = None) -> list[ForecastBundle]:
        """Run one full forecasting maintenance cycle for the requested pods.

        The order is deliberate:
        1. evaluate old forecasts that have now reached the end of their horizon
        2. backfill persistence comparison metrics if needed
        3. learn new historical cases from completed windows
        4. generate the next live forecasts

        This order keeps the case base and calibration data as up to date as
        possible before the new forecasts are issued.
        """
        cycle_time = requested_time_utc or datetime.now(timezone.utc)
        self.evaluate_due(now_utc=cycle_time, pod_ids=pod_ids)
        backfilled_count = self.backfill_persistence_metrics(now_utc=cycle_time, pod_ids=pod_ids)
        if backfilled_count:
            LOGGER.info("backfilled persistence metrics for %s historical evaluations", backfilled_count)
        learned_case_count = self.learn_due(now_utc=cycle_time, pod_ids=pod_ids)
        if learned_case_count:
            LOGGER.info("learned %s forecast cases before generating new forecasts", learned_case_count)
        bundles: list[ForecastBundle] = []
        for pod_id in pod_ids:
            bundle = self.forecast_pod(pod_id=pod_id, requested_time_utc=cycle_time)
            if bundle is not None:
                bundles.append(bundle)
        return bundles

    def forecast_pod(self, *, pod_id: str, requested_time_utc: datetime | None = None) -> ForecastBundle | None:
        """Generate and store the current forecast bundle for one pod.

        This is the single best function to open in a viva if someone asks,
        "How do we get from live sensor readings to the dashboard forecast?"
        """
        effective_time = self.adapter.effective_forecast_time(pod_id=pod_id, requested_time_utc=requested_time_utc)
        if effective_time is None:
            LOGGER.warning("No telemetry available for pod %s; skipping forecast.", pod_id)
            return None

        # Stage 1: load the latest 3-hour history window on the fixed 1-minute
        # forecasting grid.
        history = self.adapter.load_history_window(
            pod_id=pod_id,
            as_of_utc=effective_time,
            minutes=self.config.history_minutes,
        )
        if len(history.points) < self.config.history_minutes:
            LOGGER.warning(
                "Pod %s has only %s/%s resampled points; skipping forecast.",
                pod_id,
                len(history.points),
                self.config.history_minutes,
            )
            return None

        # Stage 2: decide whether the recent tail looks like a disturbance.
        event = detect_recent_event(history.points, config=self.config)
        # Stage 3: create the baseline-safe window that will be used for
        # analogue matching.
        baseline_window = build_baseline_window(history.points, detection=event, config=self.config)
        # Stage 4: compress the 3-hour window into an interpretable feature
        # vector for case-based matching.
        feature_vector = extract_feature_vector(baseline_window)
        usable_cases = self.case_base.load_cases(pod_id=pod_id, include_event_cases=False)
        # Stage 5: build the normal baseline scenario from historical analogues.
        baseline = self.knn.forecast(
            feature_vector=feature_vector,
            baseline_window=baseline_window,
            cases=usable_cases,
        )
        baseline = self._apply_recent_calibration(pod_id=pod_id, trajectory=baseline)
        # Stage 6: if the latest behaviour looked event-like, also generate the
        # "what if the disturbance continues?" scenario.
        event_persist = build_event_persist_forecast(history.points, config=self.config) if event.event_detected else None
        if event_persist is not None:
            event_persist = self._apply_recent_calibration(pod_id=pod_id, trajectory=event_persist)

        # Stage 7: package the forecast with enough context for later analysis
        # and dashboard explanation.
        bundle = ForecastBundle(
            pod_id=pod_id,
            ts_pc_utc=to_utc_iso(effective_time),
            model_version=MODEL_VERSION,
            missing_rate=feature_vector.missing_rate,
            event=event,
            feature_vector=feature_vector,
            baseline=baseline,
            event_persist=event_persist,
            metadata={
                "case_count": len(usable_cases),
                "storage_backend": self.adapter.storage_backend,
            },
        )
        self.outputs.save_bundle(bundle)
        self._buffers[pod_id] = baseline_window
        LOGGER.info(
            "forecast pod=%s ts=%s event=%s type=%s source=%s neighbors=%s missing_rate=%.3f",
            pod_id,
            bundle.ts_pc_utc,
            bundle.event.event_detected,
            bundle.event.event_type,
            bundle.baseline.source,
            bundle.baseline.neighbor_count,
            bundle.missing_rate,
        )
        return bundle

    def learn_due(self, *, now_utc: datetime | None = None, pod_ids: list[str] | None = None) -> int:
        """Learn any new analogue cases that have matured since the last run."""
        learn_time = now_utc or datetime.now(timezone.utc)
        learned = 0
        for pod_id in pod_ids or self.adapter.list_pod_ids():
            learned += self._learn_pod_cases(pod_id=pod_id, learn_time_utc=learn_time)
        return learned

    def evaluate_due(self, *, now_utc: datetime | None = None, pod_ids: list[str] | None = None) -> list[EvaluationMetrics]:
        """Evaluate stored forecasts whose full 30-minute future is now known."""
        evaluation_time = now_utc or datetime.now(timezone.utc)
        cutoff = to_utc_iso(evaluation_time - timedelta(minutes=self.config.horizon_minutes))
        pending = self.outputs.pending_evaluations(cutoff_utc=cutoff, pod_ids=pod_ids)
        evaluations: list[EvaluationMetrics] = []
        for record in pending:
            forecast_time = parse_utc(str(record["ts_pc_utc"]))
            # The actual future window is loaded from the same 1-minute grid used
            # for forecasting so the prediction and outcome are directly
            # comparable minute by minute.
            actual = self.adapter.load_actual_horizon(
                pod_id=str(record["pod_id"]),
                ts_forecast_utc=forecast_time,
                minutes=self.config.horizon_minutes,
            )
            if len(actual.points) < self.config.horizon_minutes:
                LOGGER.info(
                    "Skipping evaluation for pod=%s ts=%s; horizon data still incomplete.",
                    record["pod_id"],
                    record["ts_pc_utc"],
                )
                continue

            trajectory = self.outputs.trajectory_from_record(record)
            evaluation = evaluate_forecast(
                pod_id=str(record["pod_id"]),
                ts_forecast_utc=str(record["ts_pc_utc"]),
                trajectory=trajectory,
                actual_window=actual.points,
                event_detected=bool(record["event_detected"]),
                config=self.config,
            )
            evaluation = replace(evaluation, **self._persistence_metrics(record=record, actual_points=actual.points))
            forecast_missing_rate = self.outputs.forecast_missing_rate(record)
            notes = evaluation.notes
            if actual.missing_rate > self.config.missing_rate_max:
                notes = f"{notes};actual_missing_rate={actual.missing_rate:.3f}"
            if forecast_missing_rate > self.config.missing_rate_max:
                notes = f"{notes};forecast_missing_rate={forecast_missing_rate:.3f}"
            evaluation = replace(evaluation, notes=notes)
            self.outputs.save_evaluation(evaluation)
            evaluations.append(evaluation)
            LOGGER.info(
                "evaluation pod=%s ts=%s scenario=%s maeT=%.3f rmseT=%.3f maeRH=%.3f rmseRH=%.3f",
                evaluation.pod_id,
                evaluation.ts_forecast_utc,
                evaluation.scenario,
                evaluation.mae_temp_c,
                evaluation.rmse_temp_c,
                evaluation.mae_rh_pct,
                evaluation.rmse_rh_pct,
            )

            # Only baseline windows are added back into the case base. That keeps
            # the analogue memory aligned with ordinary storage evolution rather
            # than mixing in event-persist what-if scenarios.
            if trajectory.scenario != "baseline":
                continue
            if actual.missing_rate > self.config.missing_rate_max or forecast_missing_rate > self.config.missing_rate_max:
                continue
            self.case_base.append_case(
                CaseRecord(
                    ts_pc_utc=str(record["ts_pc_utc"]),
                    pod_id=str(record["pod_id"]),
                    feature_vector=self.outputs.feature_vector_from_record(record),
                    future_temp_c=[point.temp_c for point in actual.points[: self.config.horizon_minutes]],
                    future_rh_pct=[point.rh_pct for point in actual.points[: self.config.horizon_minutes]],
                    event_label=str(record.get("event_type") or "none") if bool(record["event_detected"]) else "none",
                )
            )
        return evaluations

    def backfill_persistence_metrics(
        self,
        *,
        now_utc: datetime | None = None,
        pod_ids: list[str] | None = None,
    ) -> int:
        """Backfill persistence comparison scores for older evaluation rows.

        This helps the dashboard compare model performance against a proper
        baseline even for historical records that were saved before those
        persistence fields existed.
        """
        evaluation_time = now_utc or datetime.now(timezone.utc)
        cutoff = to_utc_iso(evaluation_time - timedelta(minutes=self.config.horizon_minutes))
        pending = self.outputs.pending_persistence_backfill(cutoff_utc=cutoff, pod_ids=pod_ids)
        backfilled = 0
        for record in pending:
            forecast_time = parse_utc(str(record["ts_forecast_utc"]))
            actual = self.adapter.load_actual_horizon(
                pod_id=str(record["pod_id"]),
                ts_forecast_utc=forecast_time,
                minutes=self.config.horizon_minutes,
            )
            if len(actual.points) < self.config.horizon_minutes:
                continue
            updated = replace(
                self._evaluation_from_record(record),
                **self._persistence_metrics(record=record, actual_points=actual.points),
            )
            self.outputs.save_evaluation(updated)
            backfilled += 1
        return backfilled

    def _learn_pod_cases(self, *, pod_id: str, learn_time_utc: datetime) -> int:
        """Walk forward through history and append any missing matured cases.

        Each learned case uses:
        - a 3-hour feature window ending at a historical timestamp
        - the actual next 30 minutes as the answer trajectory

        This is what turns the warehouse's past telemetry into an analogue case
        base for future forecasting.
        """
        earliest = self.adapter.earliest_timestamp(pod_id)
        if earliest is None:
            return 0

        start = earliest + timedelta(minutes=self.config.history_minutes)
        latest_case = self.case_base.latest_case_timestamp(pod_id)
        if latest_case is not None:
            start = max(start, latest_case + timedelta(minutes=self.learning_interval_minutes))

        end = learn_time_utc - timedelta(minutes=self.config.horizon_minutes)
        if start > end:
            return 0

        current = floor_to_interval(start, self.learning_interval_minutes)
        if current < start:
            current += timedelta(minutes=self.learning_interval_minutes)

        learned = 0
        while current <= end:
            history = self.adapter.load_history_window(
                pod_id=pod_id,
                as_of_utc=current,
                minutes=self.config.history_minutes,
            )
            actual = self.adapter.load_actual_horizon(
                pod_id=pod_id,
                ts_forecast_utc=current,
                minutes=self.config.horizon_minutes,
            )
            if len(history.points) < self.config.history_minutes or len(actual.points) < self.config.horizon_minutes:
                current += timedelta(minutes=self.learning_interval_minutes)
                continue
            if history.missing_rate > self.config.missing_rate_max or actual.missing_rate > self.config.missing_rate_max:
                current += timedelta(minutes=self.learning_interval_minutes)
                continue

            event = detect_recent_event(history.points, config=self.config)
            feature_vector = extract_feature_vector(build_baseline_window(history.points, detection=event, config=self.config))
            self.case_base.append_case(
                CaseRecord(
                    ts_pc_utc=feature_vector.ts_pc_utc,
                    pod_id=pod_id,
                    feature_vector=feature_vector.values,
                    future_temp_c=[point.temp_c for point in actual.points[: self.config.horizon_minutes]],
                    future_rh_pct=[point.rh_pct for point in actual.points[: self.config.horizon_minutes]],
                    event_label=event.event_type if event.event_detected else "none",
                )
            )
            learned += 1
            current += timedelta(minutes=self.learning_interval_minutes)
        return learned

    def _apply_recent_calibration(self, *, pod_id: str, trajectory):
        """Apply a lightweight bias correction from recent trustworthy evaluations.

        This is intentionally modest. It does not retrain the model; it simply
        shifts the forecast if recent evaluated windows show a consistent signed
        bias. Large-error and missing-data windows are filtered out upstream so
        calibration is not driven by obviously bad evidence.
        """
        bias = self.outputs.recent_bias(
            pod_id=pod_id,
            scenario=trajectory.scenario,
            limit=self.calibration_window,
        )
        if bias is None and trajectory.scenario != "baseline":
            bias = self.outputs.recent_bias(
                pod_id=pod_id,
                scenario="baseline",
                limit=self.calibration_window,
            )
        if bias is None:
            return trajectory

        temp_shift = clamp(float(bias.temp_c), -CALIBRATION_TEMP_CAP_C, CALIBRATION_TEMP_CAP_C)
        rh_shift = clamp(float(bias.rh_pct), -CALIBRATION_RH_CAP_PCT, CALIBRATION_RH_CAP_PCT)
        if abs(temp_shift) < 0.02 and abs(rh_shift) < 0.10:
            return trajectory

        corrected_temp = [float(value) - temp_shift for value in trajectory.temp_forecast_c]
        corrected_rh = [clamp(float(value) - rh_shift, 0.0, 100.0) for value in trajectory.rh_forecast_pct]
        # Dew point is recalculated after calibration so the corrected
        # temperature/RH pair remains physically consistent.
        corrected_dew = [
            calculate_dew_point_c(temp_c, rh_pct)
            for temp_c, rh_pct in zip(corrected_temp, corrected_rh)
        ]
        correction_note = (
            f"Auto-calibrated using {bias.sample_count} recent evaluations "
            f"(temp_bias={temp_shift:+.2f}C rh_bias={rh_shift:+.2f}%)."
        )
        return replace(
            trajectory,
            temp_forecast_c=corrected_temp,
            rh_forecast_pct=corrected_rh,
            dew_point_forecast_c=corrected_dew,
            temp_p25_c=[float(value) - temp_shift for value in trajectory.temp_p25_c],
            temp_p75_c=[float(value) - temp_shift for value in trajectory.temp_p75_c],
            rh_p25_pct=[clamp(float(value) - rh_shift, 0.0, 100.0) for value in trajectory.rh_p25_pct],
            rh_p75_pct=[clamp(float(value) - rh_shift, 0.0, 100.0) for value in trajectory.rh_p75_pct],
            notes=_append_note(trajectory.notes, correction_note),
        )

    def _persistence_metrics(self, *, record: dict[str, object], actual_points: list) -> dict[str, float]:
        """Evaluate the flat persistence baseline for comparison against the model."""
        baseline = evaluate_forecast(
            pod_id=str(record["pod_id"]),
            ts_forecast_utc=str(record.get("ts_pc_utc") or record["ts_forecast_utc"]),
            trajectory=self._persistence_trajectory(record),
            actual_window=actual_points,
            event_detected=bool(record["event_detected"]),
            config=self.config,
        )
        return {
            "persistence_mae_temp_c": baseline.mae_temp_c,
            "persistence_rmse_temp_c": baseline.rmse_temp_c,
            "persistence_mae_rh_pct": baseline.mae_rh_pct,
            "persistence_rmse_rh_pct": baseline.rmse_rh_pct,
        }

    def _persistence_trajectory(self, record: dict[str, object]):
        """Construct a flat persistence trajectory from the stored forecast anchor.

        This baseline answers the simple question:
        "What if nothing changed and the latest observed state just continued?"
        """
        features = self.outputs.feature_vector_from_record(record)
        anchor_temp_c = float(features.get("temp_last", 0.0))
        anchor_rh_pct = clamp(float(features.get("rh_last", 0.0)), 0.0, 100.0)
        horizon = self.config.horizon_minutes
        dew_point_c = calculate_dew_point_c(anchor_temp_c, anchor_rh_pct)
        return replace(
            self.outputs.trajectory_from_record(record),
            scenario="persistence",
            temp_forecast_c=[anchor_temp_c for _ in range(horizon)],
            rh_forecast_pct=[anchor_rh_pct for _ in range(horizon)],
            dew_point_forecast_c=[dew_point_c for _ in range(horizon)],
            temp_p25_c=[anchor_temp_c for _ in range(horizon)],
            temp_p75_c=[anchor_temp_c for _ in range(horizon)],
            rh_p25_pct=[anchor_rh_pct for _ in range(horizon)],
            rh_p75_pct=[anchor_rh_pct for _ in range(horizon)],
            source="persistence_baseline",
            neighbor_count=0,
            case_count=0,
            notes="Flat persistence baseline anchored to the latest observed reading.",
        )

    @staticmethod
    def _evaluation_from_record(record: dict[str, object]) -> EvaluationMetrics:
        """Recreate an ``EvaluationMetrics`` object from a stored row."""
        return EvaluationMetrics(
            ts_forecast_utc=str(record["ts_forecast_utc"]),
            pod_id=str(record["pod_id"]),
            scenario=str(record["scenario"]),
            mae_temp_c=float(record["MAE_T"]),
            rmse_temp_c=float(record["RMSE_T"]),
            mae_rh_pct=float(record["MAE_RH"]),
            rmse_rh_pct=float(record["RMSE_RH"]),
            bias_temp_c=float(record.get("BIAS_T") or 0.0),
            bias_rh_pct=float(record.get("BIAS_RH") or 0.0),
            event_detected=bool(record["event_detected"]),
            large_error=bool(record["large_error"]),
            persistence_mae_temp_c=(
                None if record.get("PERSIST_MAE_T") is None else float(record["PERSIST_MAE_T"])
            ),
            persistence_rmse_temp_c=(
                None if record.get("PERSIST_RMSE_T") is None else float(record["PERSIST_RMSE_T"])
            ),
            persistence_mae_rh_pct=(
                None if record.get("PERSIST_MAE_RH") is None else float(record["PERSIST_MAE_RH"])
            ),
            persistence_rmse_rh_pct=(
                None if record.get("PERSIST_RMSE_RH") is None else float(record["PERSIST_RMSE_RH"])
            ),
            notes=str(record.get("notes") or ""),
        )


def _append_note(existing: str, addition: str) -> str:
    """Append a short human-readable note to stored forecast metadata."""
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing}; {addition}"
