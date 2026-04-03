"""Forecasting pipeline runner that connects gateway storage to the ML package."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone

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
from forecasting.models import CaseRecord, ForecastBundle, EvaluationMetrics
from forecasting.utils import parse_utc, to_utc_iso


LOGGER = logging.getLogger(__name__)


class ForecastRunner:
    """Run one-shot or continuous per-pod forecasting cycles."""

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
        self.outputs.ensure_storage()
        self.case_base.ensure_storage()

    @property
    def active_storage_backend(self) -> str:
        return self.adapter.storage_backend

    def pod_ids(self, requested_pods: list[str] | None = None, use_all: bool = False) -> list[str]:
        if use_all:
            return self.adapter.list_pod_ids()
        return sorted(set(requested_pods or []))

    def run_cycle(self, *, pod_ids: list[str], requested_time_utc: datetime | None = None) -> list[ForecastBundle]:
        cycle_time = requested_time_utc or datetime.now(timezone.utc)
        self.evaluate_due(now_utc=cycle_time, pod_ids=pod_ids)
        bundles: list[ForecastBundle] = []
        for pod_id in pod_ids:
            bundle = self.forecast_pod(pod_id=pod_id, requested_time_utc=cycle_time)
            if bundle is not None:
                bundles.append(bundle)
        return bundles

    def forecast_pod(self, *, pod_id: str, requested_time_utc: datetime | None = None) -> ForecastBundle | None:
        effective_time = self.adapter.effective_forecast_time(pod_id=pod_id, requested_time_utc=requested_time_utc)
        if effective_time is None:
            LOGGER.warning("No telemetry available for pod %s; skipping forecast.", pod_id)
            return None

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

        event = detect_recent_event(history.points, config=self.config)
        baseline_window = build_baseline_window(history.points, detection=event, config=self.config)
        feature_vector = extract_feature_vector(baseline_window)
        usable_cases = self.case_base.load_cases(pod_id=pod_id, include_event_cases=False)
        baseline = self.knn.forecast(
            feature_vector=feature_vector,
            baseline_window=baseline_window,
            cases=usable_cases,
        )
        event_persist = build_event_persist_forecast(history.points, config=self.config) if event.event_detected else None

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

    def evaluate_due(self, *, now_utc: datetime | None = None, pod_ids: list[str] | None = None) -> list[EvaluationMetrics]:
        evaluation_time = now_utc or datetime.now(timezone.utc)
        cutoff = to_utc_iso(evaluation_time - timedelta(minutes=self.config.horizon_minutes))
        pending = self.outputs.pending_evaluations(cutoff_utc=cutoff, pod_ids=pod_ids)
        evaluations: list[EvaluationMetrics] = []
        for record in pending:
            forecast_time = parse_utc(str(record["ts_pc_utc"]))
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
