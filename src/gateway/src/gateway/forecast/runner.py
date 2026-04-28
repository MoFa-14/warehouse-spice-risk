# File overview:
# - Responsibility: Operational forecast runner for the integrated gateway pipeline.
# - Project role: Connects stored telemetry to forecasting, persistence, evaluation,
#   and calibration behavior.
# - Main data or concerns: History windows, forecast bundles, evaluation rows, and
#   calibration metadata.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

"""Operational forecast runner for the integrated gateway pipeline.

Responsibilities:
- Orchestrates the full forecast lifecycle for each pod.
- Connects live telemetry storage, preprocessing, model execution, persistence,
  evaluation, case learning, and light calibration.

Project flow:
- telemetry storage -> 3-hour history window -> event detection -> baseline
  features -> baseline forecast (+ optional event-persist) -> stored bundle ->
  later evaluation -> case learning -> recent-bias calibration

Why this matters:
- The modelling package explains how one forecast is produced.
- This file explains when forecasts are issued, how due evaluations are handled,
  and how completed windows feed back into future forecasting.
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


# Forecast lifecycle orchestrator
# - Purpose: coordinates forecasting work for one or more pods across repeated
#   cycles.
# - Project role: top-level service called by CLI entry points and automation
#   scripts.
# - Inputs: storage configuration, optional timing overrides, and requested pod
#   lists.
# - Outputs: stored forecast bundles, stored evaluations, learned cases, and
#   lightweight calibration effects on new trajectories.
# - Related flow:
#   - reads telemetry through ``ForecastStorageAdapter``
#   - builds outputs through the forecasting package
#   - persists bundles and evaluations through ``ForecastOutputs``
# Class purpose: Run one-shot or continuous per-pod forecasting cycles.
# - Project role: Belongs to the gateway forecast orchestration layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

class ForecastRunner:
    """Run one-shot or continuous per-pod forecasting cycles."""

    # Runner initialisation
    # - Purpose: wires together configuration, telemetry access, output
    #   persistence, case storage, and the analogue forecaster.
    # - Project role: bootstrap stage for the forecasting service.
    # Method purpose: Initialise the runner and connect all forecasting
    #   subsystems.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as storage_backend, db_path, data_root,
    #   adjustments_path, k, history_minutes, horizon_minutes, missing_rate_max,
    #   interpreted according to the implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

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
        """Initialise the runner and connect all forecasting subsystems."""
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

    # Method purpose: Handles storage backend for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns str when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    @property
    def active_storage_backend(self) -> str:
        return self.adapter.storage_backend

    # Method purpose: Handles target for the surrounding project flow.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    @property
    def lock_target(self) -> Path:
        if self.adapter.storage_backend == "sqlite":
            return self.outputs.db_path.parent / "forecast_runner.sqlite"
        return self.outputs.forecasts_jsonl.parent / "forecast_runner.sqlite"

    # Pod selection
    # - Purpose: resolves whether the current run should use the requested pod
    #   list or discover all available pods from storage.
    # Method purpose: Resolve the list of pods the current run should operate
    #   on.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as requested_pods, use_all, interpreted according
    #   to the implementation below.
    # - Outputs: Returns list[str] when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def pod_ids(self, requested_pods: list[str] | None = None, use_all: bool = False) -> list[str]:
        """Resolve the list of pods the current run should operate on."""
        if use_all:
            return self.adapter.list_pod_ids()
        return sorted(set(requested_pods or []))

    # Full maintenance cycle
    # - Purpose: executes the ordered forecast maintenance steps for the chosen
    #   pods.
    # - Project role: main scheduler-facing entry point for periodic operation.
    # - Inputs: pod list and an optional cycle time override.
    # - Outputs: newly generated forecast bundles.
    # - Flow:
    #   1. evaluate completed forecasts
    #   2. backfill persistence metrics on older rows when needed
    #   3. learn new analogue cases from matured windows
    #   4. generate the next live forecast bundles
    # Method purpose: Run one full forecasting maintenance cycle for the
    #   requested pods.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as pod_ids, requested_time_utc, interpreted
    #   according to the implementation below.
    # - Outputs: Returns list[ForecastBundle] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def run_cycle(self, *, pod_ids: list[str], requested_time_utc: datetime | None = None) -> list[ForecastBundle]:
        """Run one full forecasting maintenance cycle for the requested pods."""
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

    # Per-pod forecast generation
    # - Purpose: produces and stores the current forecast bundle for one pod.
    # - Project role: end-to-end live forecast path from stored telemetry to
    #   persisted scenarios.
    # - Inputs: pod identifier and optional requested forecast time.
    # - Outputs: one stored ``ForecastBundle`` or ``None`` when telemetry is not
    #   yet sufficient.
    # - Related flow:
    #   telemetry -> history window -> event detection -> baseline features ->
    #   baseline forecast -> optional event-persist -> stored bundle
    # Method purpose: Generate and store the current forecast bundle for one
    #   pod.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as pod_id, requested_time_utc, interpreted
    #   according to the implementation below.
    # - Outputs: Returns ForecastBundle | None when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def forecast_pod(self, *, pod_id: str, requested_time_utc: datetime | None = None) -> ForecastBundle | None:
        """Generate and store the current forecast bundle for one pod."""
        effective_time = self.adapter.effective_forecast_time(pod_id=pod_id, requested_time_utc=requested_time_utc)
        if effective_time is None:
            LOGGER.warning("No telemetry available for pod %s; skipping forecast.", pod_id)
            return None

        # Stage 1: load the latest 3-hour history window on the fixed 1-minute
        # forecasting grid shared by live forecasting and historical learning.
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
        # Stage 4: compress the 3-hour window into the interpretable feature
        # vector used for case-based similarity matching.
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

        # Stage 7: package the forecast with enough context for later storage,
        # evaluation, dashboard explanation, and historical analysis.
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

    # Due-case learning
    # - Purpose: scans pods for completed windows that can now be added to the
    #   analogue case base.
    # Method purpose: Learn any new analogue cases that have matured since the
    #   last run.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as now_utc, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns int when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def learn_due(self, *, now_utc: datetime | None = None, pod_ids: list[str] | None = None) -> int:
        """Learn any new analogue cases that have matured since the last run."""
        learn_time = now_utc or datetime.now(timezone.utc)
        learned = 0
        for pod_id in pod_ids or self.adapter.list_pod_ids():
            learned += self._learn_pod_cases(pod_id=pod_id, learn_time_utc=learn_time)
        return learned

    # Due-evaluation pass
    # - Purpose: evaluates forecast rows whose realised future is now fully
    #   available.
    # - Project role: closes the loop between stored forecast output and
    #   realised performance evidence.
    # - Outputs: saved ``EvaluationMetrics`` records and, for suitable baseline
    #   windows, new appended analogue cases.
    # Method purpose: Evaluate stored forecasts whose full 30-minute future is
    #   now known.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as now_utc, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns list[EvaluationMetrics] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def evaluate_due(self, *, now_utc: datetime | None = None, pod_ids: list[str] | None = None) -> list[EvaluationMetrics]:
        """Evaluate stored forecasts whose full 30-minute future is now known."""
        evaluation_time = now_utc or datetime.now(timezone.utc)
        cutoff = to_utc_iso(evaluation_time - timedelta(minutes=self.config.horizon_minutes))
        pending = self.outputs.pending_evaluations(cutoff_utc=cutoff, pod_ids=pod_ids)
        evaluations: list[EvaluationMetrics] = []
        for record in pending:
            forecast_time = parse_utc(str(record["ts_pc_utc"]))
            # Actual future data is reconstructed on the same 1-minute grid used
            # for forecasting so every stored prediction step has a directly
            # comparable realised counterpart.
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

            # Only baseline windows are learned back into the case base. The
            # analogue memory should represent realised storage evolution rather
            # than alternate event-persist what-if trajectories.
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

    # Persistence backfill
    # - Purpose: fills in persistence-comparison metrics on older evaluation
    #   rows saved before those fields existed.
    # - Downstream dependency: the dashboard comparison chart expects these
    #   fields for historical model-vs-persistence review.
    # Method purpose: Backfill persistence comparison scores for older
    #   evaluation rows.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as now_utc, pod_ids, interpreted according to the
    #   implementation below.
    # - Outputs: Returns int when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def backfill_persistence_metrics(
        self,
        *,
        now_utc: datetime | None = None,
        pod_ids: list[str] | None = None,
    ) -> int:
        """Backfill persistence comparison scores for older evaluation rows."""
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

    # Historical case extraction
    # - Purpose: walks through one pod's historical timeline and appends any
    #   missing matured analogue cases.
    # - Project role: converts past telemetry into reusable historical memory.
    # - Inputs: pod identifier and the current learning cutoff time.
    # - Outputs: count of newly stored cases.
    # Method purpose: Walk forward through history and append any missing
    #   matured cases.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as pod_id, learn_time_utc, interpreted according
    #   to the implementation below.
    # - Outputs: Returns int when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _learn_pod_cases(self, *, pod_id: str, learn_time_utc: datetime) -> int:
        """Walk forward through history and append any missing matured cases."""
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

    # Lightweight recent-bias calibration
    # - Purpose: shifts a newly generated trajectory using recent trustworthy
    #   signed bias estimates.
    # - Project role: post-processing stage after trajectory generation and
    #   before the bundle is stored.
    # - Important decisions: calibration is capped, only trusted evaluations are
    #   used upstream, and dew point is recomputed after temperature/RH shifts.
    # Method purpose: Apply a lightweight bias correction from recent
    #   trustworthy evaluations.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as pod_id, trajectory, interpreted according to
    #   the implementation below.
    # - Outputs: Returns the value or side effect defined by the implementation.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _apply_recent_calibration(self, *, pod_id: str, trajectory):
        """Apply a lightweight bias correction from recent trustworthy evaluations."""
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

    # Persistence baseline scoring
    # - Purpose: evaluates a flat anchor-hold baseline against the realised
    #   future so model skill can be compared against a trivial forecast.
    # Method purpose: Evaluate the flat persistence baseline for comparison
    #   against the model.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as record, actual_points, interpreted according
    #   to the implementation below.
    # - Outputs: Returns dict[str, float] when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

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

    # Persistence trajectory construction
    # - Purpose: rebuilds the simple baseline that holds the last observed state
    #   flat across the whole horizon.
    # - Downstream dependency: used during evaluation and dashboard model-vs-
    #   persistence comparisons.
    # Method purpose: Construct a flat persistence trajectory from the stored
    #   forecast anchor.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as record, interpreted according to the
    #   implementation below.
    # - Outputs: Returns the value or side effect defined by the implementation.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

    def _persistence_trajectory(self, record: dict[str, object]):
        """Construct a flat persistence trajectory from the stored forecast anchor."""
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

    # Evaluation row reconstruction
    # - Purpose: converts one stored evaluation row back into the in-memory
    #   ``EvaluationMetrics`` structure for persistence backfill updates.
    # Method purpose: Recreate an ``EvaluationMetrics`` object from a stored
    #   row.
    # - Project role: Belongs to the gateway forecast orchestration layer and
    #   acts as a method on ForecastRunner.
    # - Inputs: Arguments such as record, interpreted according to the
    #   implementation below.
    # - Outputs: Returns EvaluationMetrics when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Receives normalized telemetry windows and passes stored
    #   forecasts and evaluations to later dashboard reads.

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


# Forecast-note helper
# - Purpose: appends one explanatory note to stored metadata without
#   duplicating the same sentence.
# Function purpose: Append a short human-readable note to stored forecast metadata.
# - Project role: Belongs to the gateway forecast orchestration layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as existing, addition, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Receives normalized telemetry windows and passes stored forecasts
#   and evaluations to later dashboard reads.

def _append_note(existing: str, addition: str) -> str:
    """Append a short human-readable note to stored forecast metadata."""
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing}; {addition}"
