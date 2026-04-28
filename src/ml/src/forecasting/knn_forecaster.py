# File overview:
# - Responsibility: Analogue / kNN trajectory forecasting.
# - Project role: Defines feature extraction, case matching, scenario generation,
#   evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

"""Analogue / kNN trajectory forecasting.

Responsibilities:
- Builds the baseline forecast from the current feature vector and stored case
  history.
- Converts historical similarity into a future trajectory for temperature, RH,
  and derived dew point.
- Applies interpretable safeguards when analogue support is sparse or drawn
  from an incompatible humidity regime.

Project flow:
- baseline window -> feature vector -> case retrieval -> distance scoring ->
  neighbour aggregation -> baseline forecast trajectory

Why this matters:
- This file contains the core predictive logic of the baseline scenario.
- The model remains intentionally inspectable: every major step corresponds to
  a traceable engineering decision rather than an opaque latent model.
"""

from __future__ import annotations

from datetime import datetime
from math import sqrt

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import CaseRecord, FeatureVector, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import clamp, mean, parse_utc, percentile, population_std


# Baseline analogue forecaster
# - Purpose: converts the current feature vector and case base into a baseline
#   multi-step forecast.
# - Project role: main prediction stage after preprocessing, event detection,
#   and feature extraction have already completed.
# - Inputs: current feature vector, baseline-safe history window, and stored
#   analogue cases.
# - Outputs: ``ForecastTrajectory`` for the baseline scenario.
# - Important decisions: filters unusable cases, gates incompatible humidity
#   regimes, scores neighbours in normalised feature space, and falls back to
#   bounded persistence when analogue support is weak.
# Class purpose: Forecast future temperature and RH using stored analogue cases.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related behavior behind one stateful interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

class AnalogueKNNForecaster:
    """Forecast future temperature and RH using stored analogue cases."""

    # Method purpose: Handles init for the surrounding project flow.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on AnalogueKNNForecaster.
    # - Inputs: Arguments such as config, interpreted according to the
    #   implementation below.
    # - Outputs: Returns None when the function completes successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def __init__(self, *, config: ForecastConfig) -> None:
        self.config = config

    # Baseline forecast generation
    # - Purpose: produces the stored baseline scenario for one pod and one
    #   forecast timestamp.
    # - Inputs: the current feature vector, the filtered baseline window, and
    #   the historical case pool for the same pod.
    # - Outputs: ``ForecastTrajectory`` with forecast path, uncertainty bands,
    #   provenance, and explanatory notes.
    # - Important decisions:
    #   - reject cases that do not contain a full forecast horizon
    #   - reject cases from clearly different RH/dew regimes
    #   - apply a small recency preference when distances are otherwise close
    #   - blend RH back toward persistence when analogue support is too thin
    # - Downstream dependency: the forecast runner stores this trajectory and
    #   later evaluates it against realised telemetry.
    # Method purpose: Build the baseline forecast for one pod and one timestamp.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on AnalogueKNNForecaster.
    # - Inputs: Arguments such as feature_vector, baseline_window, cases,
    #   interpreted according to the implementation below.
    # - Outputs: Returns ForecastTrajectory when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        cases: list[CaseRecord],
    ) -> ForecastTrajectory:
        """Build the baseline forecast for one pod and one timestamp."""
        usable_cases = [
            case
            for case in cases
            if len(case.future_temp_c) >= self.config.horizon_minutes and len(case.future_rh_pct) >= self.config.horizon_minutes
        ]
        if len(usable_cases) < self.config.minimum_case_count:
            return self._fallback_forecast(
                feature_vector=feature_vector,
                baseline_window=baseline_window,
                case_count=len(usable_cases),
            )

        # Distance normalisation
        # Feature statistics are derived from the currently usable case pool so
        # larger-scale features do not dominate only because of their units.
        means, stds = _feature_stats(usable_cases)
        compatible_cases = [
            case
            for case in usable_cases
            if _is_rh_regime_compatible(
                current=feature_vector.values,
                candidate=case.feature_vector,
                config=self.config,
            )
        ]
        # Regime gating
        # RH/dew gating discards analogues that look numerically close in the
        # full feature space but belong to a clearly different moisture regime.
        rejected_count = len(usable_cases) - len(compatible_cases)
        if not compatible_cases:
            return _build_support_fallback(
                baseline_window=baseline_window,
                scenario="baseline",
                horizon_minutes=self.config.horizon_minutes,
                case_count=len(usable_cases),
                notes=(
                    f"Rejected all {len(usable_cases)} analogue cases on RH/dew regime gates; "
                    "used flat persistence until similar history accumulates."
                ),
            )

        # Ranking
        # Distance is combined with a small age penalty so newer but otherwise
        # equally similar cases receive preference without turning the model
        # into an explicitly time-conditioned learner.
        current_ts = parse_utc(feature_vector.ts_pc_utc)
        scored_cases = sorted(
            (
                _distance(
                    current=feature_vector.values,
                    candidate=case.feature_vector,
                    means=means,
                    stds=stds,
                    weights=self.config.feature_weights,
                )
                + _recency_penalty(
                    current_ts=current_ts,
                    candidate_ts=parse_utc(case.ts_pc_utc),
                    config=self.config,
                ),
                case,
            )
            for case in compatible_cases
        )
        neighbors = [case for _, case in scored_cases[: min(self.config.knn_k, len(scored_cases))]]
        forecast = _aggregate_neighbors(
            neighbors,
            scenario="baseline",
            anchor_temp_c=float(feature_vector.values.get("temp_last", baseline_window[-1].temp_c)),
            anchor_rh_pct=float(feature_vector.values.get("rh_last", baseline_window[-1].rh_pct)),
        )
        # Support-strength adjustment
        # RH support can remain thin even after neighbour selection. In that
        # case the forecast keeps the analogue temperature path but tempers the
        # humidity path toward persistence to avoid overstating confidence.
        weak_support_threshold = _weak_rh_support_threshold(self.config.minimum_case_count)
        if len(neighbors) < weak_support_threshold:
            support_weight = _rh_support_weight(
                neighbor_count=len(neighbors),
                minimum_case_count=self.config.minimum_case_count,
            )
            forecast = _blend_rh_with_persistence(
                trajectory=forecast,
                baseline_window=baseline_window,
                analogue_weight=support_weight,
                notes=(
                    f"Weak RH analogue support ({len(neighbors)}/{self.config.minimum_case_count}) after "
                    f"rejecting {rejected_count} cases on RH/dew regime gates; blended RH toward persistence."
                ),
            )
        elif rejected_count:
            forecast = _replace_notes(
                forecast,
                _append_note(
                    forecast.notes,
                    f"Rejected {rejected_count} cases on RH/dew regime gates before neighbour selection.",
                ),
            )
        return forecast

    # Sparse-support fallback
    # - Purpose: returns a bounded persistence-style path when the case base is
    #   too small to support analogue forecasting.
    # - Project role: safety fallback used before any neighbour-based forecast
    #   can be trusted.
    # - Inputs: current feature vector, baseline window, and available case
    #   count.
    # - Outputs: deterministic fallback trajectory with widening uncertainty
    #   bands.
    # Method purpose: Return a bounded slope-persistence fallback when case
    #   support is weak.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on AnalogueKNNForecaster.
    # - Inputs: Arguments such as feature_vector, baseline_window, case_count,
    #   interpreted according to the implementation below.
    # - Outputs: Returns ForecastTrajectory when the function completes
    #   successfully.
    # - Design reason: Forecast-facing code needs explicit documentation because
    #   later evaluation, storage, and dashboard layers depend on the exact
    #   transformation path.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or evaluation artefacts to gateway orchestration.

    def _fallback_forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        case_count: int,
    ) -> ForecastTrajectory:
        """Return a bounded slope-persistence fallback when case support is weak."""
        temp_last = baseline_window[-1].temp_c
        rh_last = baseline_window[-1].rh_pct
        temp_rate = clamp(
            feature_vector.values.get("temp_slope_30", 0.0),
            -self.config.baseline_temp_rate_cap_c_per_min,
            self.config.baseline_temp_rate_cap_c_per_min,
        )
        rh_rate = clamp(
            feature_vector.values.get("rh_slope_30", 0.0),
            -self.config.baseline_rh_rate_cap_pct_per_min,
            self.config.baseline_rh_rate_cap_pct_per_min,
        )
        temp_vol = max(population_std([point.temp_c for point in baseline_window[-30:]]) * 0.75, self.config.fallback_temp_band_c)
        rh_vol = max(population_std([point.rh_pct for point in baseline_window[-30:]]) * 0.75, self.config.fallback_rh_band_pct)

        temp_forecast: list[float] = []
        rh_forecast: list[float] = []
        dew_forecast: list[float] = []
        temp_p25: list[float] = []
        temp_p75: list[float] = []
        rh_p25: list[float] = []
        rh_p75: list[float] = []
        for step in range(1, self.config.horizon_minutes + 1):
            widening = sqrt(step / float(self.config.horizon_minutes))
            temp_value = temp_last + temp_rate * step
            rh_value = clamp(rh_last + rh_rate * step, 0.0, 100.0)
            # Dew point is recalculated from the evolving temperature/RH pair so
            # the fallback remains physically consistent rather than treating dew
            # point as an independent target.
            temp_forecast.append(temp_value)
            rh_forecast.append(rh_value)
            dew_forecast.append(calculate_dew_point_c(temp_value, rh_value))
            temp_p25.append(temp_value - temp_vol * widening)
            temp_p75.append(temp_value + temp_vol * widening)
            rh_p25.append(max(0.0, rh_value - rh_vol * widening))
            rh_p75.append(min(100.0, rh_value + rh_vol * widening))

        return ForecastTrajectory(
            scenario="baseline",
            temp_forecast_c=temp_forecast,
            rh_forecast_pct=rh_forecast,
            dew_point_forecast_c=dew_forecast,
            temp_p25_c=temp_p25,
            temp_p75_c=temp_p75,
            rh_p25_pct=rh_p25,
            rh_p75_pct=rh_p75,
            source="fallback_persistence",
            neighbor_count=0,
            case_count=case_count,
            notes="Case base smaller than minimum analogue threshold; used bounded slope persistence.",
        )


# Feature-pool statistics
# - Purpose: derives the normalisation statistics used by the distance metric.
# - Design reason: standardising by the observed case pool keeps the distance
#   measure comparable across features with different units and spreads.
# Function purpose: Compute mean and spread for each feature across the available
#   case pool.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as cases, interpreted according to the implementation
#   below.
# - Outputs: Returns tuple[dict[str, float], dict[str, float]] when the function
#   completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _feature_stats(cases: list[CaseRecord]) -> tuple[dict[str, float], dict[str, float]]:
    """Compute mean and spread for each feature across the available case pool."""
    keys = sorted(cases[0].feature_vector.keys())
    means: dict[str, float] = {}
    stds: dict[str, float] = {}
    for key in keys:
        values = [float(case.feature_vector.get(key, 0.0)) for case in cases]
        means[key] = mean(values)
        stds[key] = max(population_std(values), 1e-6)
    return means, stds


# Similarity score
# - Purpose: computes the weighted distance between the current situation and a
#   historical case in normalised feature space.
# - Project role: ranking stage that determines which cases qualify as nearest
#   neighbours.
# - Important decision: feature weights encode project-specific importance while
#   still operating on standardised values.
# Function purpose: Compute weighted Euclidean distance in normalised feature space.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as current, candidate, means, stds, weights, interpreted
#   according to the implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _distance(
    *,
    current: dict[str, float],
    candidate: dict[str, float],
    means: dict[str, float],
    stds: dict[str, float],
    weights: dict[str, float],
) -> float:
    """Compute weighted Euclidean distance in normalised feature space."""
    total = 0.0
    for key, current_value in current.items():
        if key not in candidate or key not in means or key not in stds:
            continue
        weight = weights.get(key, 1.0)
        normalized_current = (float(current_value) - means[key]) / stds[key]
        normalized_candidate = (float(candidate[key]) - means[key]) / stds[key]
        total += weight * (normalized_current - normalized_candidate) ** 2
    return sqrt(total)


# RH/dew regime gate
# - Purpose: rejects analogue candidates that belong to a clearly different
#   moisture regime.
# - Design reason: full-feature distance alone can still choose cases with an
#   implausible RH/dew anchor, which distorts the forecast starting regime.
# Function purpose: Reject cases that live in a clearly different humidity regime.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as current, candidate, config, interpreted according to
#   the implementation below.
# - Outputs: Returns bool when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _is_rh_regime_compatible(
    *,
    current: dict[str, float],
    candidate: dict[str, float],
    config: ForecastConfig,
) -> bool:
    """Reject cases that live in a clearly different humidity regime."""
    current_rh = current.get("rh_last")
    current_dew = current.get("dew_last")
    candidate_rh = candidate.get("rh_last")
    candidate_dew = candidate.get("dew_last")
    if current_rh is None or current_dew is None or candidate_rh is None or candidate_dew is None:
        return True
    return (
        abs(float(candidate_rh) - float(current_rh)) <= config.analogue_rh_gate_pct
        and abs(float(candidate_dew) - float(current_dew)) <= config.analogue_dew_gate_c
    )


# Recency preference
# - Purpose: lightly penalises older cases when feature similarity is otherwise
#   close.
# - Project role: tie-break style adjustment layered on top of feature distance.
# Function purpose: Add a small age penalty so newer cases are preferred when
#   similarity is close.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as current_ts, candidate_ts, config, interpreted
#   according to the implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _recency_penalty(
    *,
    current_ts: datetime,
    candidate_ts: datetime,
    config: ForecastConfig,
) -> float:
    """Add a small age penalty so newer cases are preferred when similarity is close."""
    age_days = max((current_ts - candidate_ts).total_seconds() / 86400.0, 0.0)
    return min(age_days * config.analogue_recency_penalty_per_day, config.analogue_recency_penalty_cap)


# RH confidence weight
# - Purpose: converts the neighbour count into a smooth blend weight for RH.
# - Design reason: thin support should reduce confidence gradually rather than
#   forcing an all-or-nothing switch between analogue output and persistence.
# Function purpose: Convert analogue support into a smooth RH blending weight.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as neighbor_count, minimum_case_count, interpreted
#   according to the implementation below.
# - Outputs: Returns float when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _rh_support_weight(*, neighbor_count: int, minimum_case_count: int) -> float:
    """Convert analogue support into a smooth RH blending weight."""
    if minimum_case_count <= 1:
        return 1.0 if neighbor_count > 0 else 0.0
    missing_support = max(minimum_case_count - neighbor_count, 0)
    return clamp(1.0 - (missing_support / float(minimum_case_count)) ** 2, 0.0, 1.0)


# Weak-support threshold
# - Purpose: defines the neighbour-count boundary below which RH blending is
#   applied.
# Function purpose: Define when RH analogue support is weak enough to justify
#   blending.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as minimum_case_count, interpreted according to the
#   implementation below.
# - Outputs: Returns int when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _weak_rh_support_threshold(minimum_case_count: int) -> int:
    """Define when RH analogue support is weak enough to justify blending."""
    if minimum_case_count <= 1:
        return 1
    return max(2, minimum_case_count - 1)


# Neighbour aggregation
# - Purpose: converts the selected neighbour futures into one baseline
#   trajectory.
# - Project role: final aggregation stage of the analogue forecast path.
# - Inputs: chosen neighbour cases plus the current live anchor state.
# - Outputs: median forecast path and interquartile bands.
# - Important decision: aggregates re-anchored offsets rather than raw
#   absolute values so the forecast reuses historical change patterns without
#   inheriting another case's absolute regime level.
# Function purpose: Aggregate neighbour futures into one baseline forecast
#   trajectory.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as neighbors, scenario, anchor_temp_c, anchor_rh_pct,
#   interpreted according to the implementation below.
# - Outputs: Returns ForecastTrajectory when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _aggregate_neighbors(
    neighbors: list[CaseRecord],
    *,
    scenario: str,
    anchor_temp_c: float,
    anchor_rh_pct: float,
) -> ForecastTrajectory:
    """Aggregate neighbour futures into one baseline forecast trajectory."""
    horizon = min(len(case.future_temp_c) for case in neighbors)
    temp_forecast: list[float] = []
    rh_forecast: list[float] = []
    dew_forecast: list[float] = []
    temp_p25: list[float] = []
    temp_p75: list[float] = []
    rh_p25: list[float] = []
    rh_p75: list[float] = []

    for step in range(horizon):
        # Each neighbour contributes its pattern of change relative to its own
        # anchor rather than its absolute future level.
        temp_offsets = [
            float(case.future_temp_c[step]) - float(case.feature_vector.get("temp_last", case.future_temp_c[0]))
            for case in neighbors
        ]
        rh_offsets = [
            float(case.future_rh_pct[step]) - float(case.feature_vector.get("rh_last", case.future_rh_pct[0]))
            for case in neighbors
        ]
        # Median aggregation limits the influence of one unusual future path.
        temp_mid = anchor_temp_c + percentile(temp_offsets, 50.0)
        rh_mid = clamp(anchor_rh_pct + percentile(rh_offsets, 50.0), 0.0, 100.0)
        temp_forecast.append(temp_mid)
        rh_forecast.append(rh_mid)
        dew_forecast.append(calculate_dew_point_c(temp_mid, rh_mid))
        temp_p25.append(anchor_temp_c + percentile(temp_offsets, 25.0))
        temp_p75.append(anchor_temp_c + percentile(temp_offsets, 75.0))
        rh_p25.append(clamp(anchor_rh_pct + percentile(rh_offsets, 25.0), 0.0, 100.0))
        rh_p75.append(clamp(anchor_rh_pct + percentile(rh_offsets, 75.0), 0.0, 100.0))

    return ForecastTrajectory(
        scenario=scenario,
        temp_forecast_c=temp_forecast,
        rh_forecast_pct=rh_forecast,
        dew_point_forecast_c=dew_forecast,
        temp_p25_c=temp_p25,
        temp_p75_c=temp_p75,
        rh_p25_pct=rh_p25,
        rh_p75_pct=rh_p75,
        source="analogue_knn",
        neighbor_count=len(neighbors),
        case_count=len(neighbors),
        notes=f"Median re-anchored delta forecast over {len(neighbors)} nearest historical cases.",
    )


# Regime-support fallback
# - Purpose: returns a flat persistence path when all otherwise usable cases are
#   rejected by RH/dew regime gating.
# - Design reason: once regime compatibility fails, a neutral anchor-hold path
#   is safer than forcing a forecast from mismatched analogues.
# Function purpose: Build a flat persistence-style fallback when no RH-compatible
#   cases exist.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as baseline_window, scenario, horizon_minutes,
#   case_count, notes, interpreted according to the implementation below.
# - Outputs: Returns ForecastTrajectory when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _build_support_fallback(
    *,
    baseline_window: list[TimeSeriesPoint],
    scenario: str,
    horizon_minutes: int,
    case_count: int,
    notes: str,
) -> ForecastTrajectory:
    """Build a flat persistence-style fallback when no RH-compatible cases exist."""
    temp_last = float(baseline_window[-1].temp_c)
    rh_last = clamp(float(baseline_window[-1].rh_pct), 0.0, 100.0)
    dew_last = calculate_dew_point_c(temp_last, rh_last)
    return ForecastTrajectory(
        scenario=scenario,
        temp_forecast_c=[temp_last for _ in range(horizon_minutes)],
        rh_forecast_pct=[rh_last for _ in range(horizon_minutes)],
        dew_point_forecast_c=[dew_last for _ in range(horizon_minutes)],
        temp_p25_c=[temp_last for _ in range(horizon_minutes)],
        temp_p75_c=[temp_last for _ in range(horizon_minutes)],
        rh_p25_pct=[rh_last for _ in range(horizon_minutes)],
        rh_p75_pct=[rh_last for _ in range(horizon_minutes)],
        source="persistence_support_fallback",
        neighbor_count=0,
        case_count=case_count,
        notes=notes,
    )


# RH support blending
# - Purpose: tempers the RH forecast toward persistence while preserving the
#   analogue temperature path.
# - Project role: post-processing safeguard applied when humidity support is
#   present but judged weaker than temperature support.
# - Important decision: dew point is recomputed after blending so the adjusted
#   trajectory remains physically consistent.
# Function purpose: Blend analogue RH toward persistence while leaving the
#   temperature path intact.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as trajectory, baseline_window, analogue_weight, notes,
#   interpreted according to the implementation below.
# - Outputs: Returns ForecastTrajectory when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _blend_rh_with_persistence(
    *,
    trajectory: ForecastTrajectory,
    baseline_window: list[TimeSeriesPoint],
    analogue_weight: float,
    notes: str,
) -> ForecastTrajectory:
    """Blend analogue RH toward persistence while leaving the temperature path intact."""
    weight = clamp(float(analogue_weight), 0.0, 1.0)
    anchor_rh = clamp(float(baseline_window[-1].rh_pct), 0.0, 100.0)
    blended_rh = [
        clamp(anchor_rh + weight * (float(value) - anchor_rh), 0.0, 100.0)
        for value in trajectory.rh_forecast_pct
    ]
    blended_rh_p25 = [
        clamp(anchor_rh + weight * (float(value) - anchor_rh), 0.0, 100.0)
        for value in trajectory.rh_p25_pct
    ]
    blended_rh_p75 = [
        clamp(anchor_rh + weight * (float(value) - anchor_rh), 0.0, 100.0)
        for value in trajectory.rh_p75_pct
    ]
    blended_dew = [
        calculate_dew_point_c(temp_c, rh_pct)
        for temp_c, rh_pct in zip(trajectory.temp_forecast_c, blended_rh)
    ]
    return ForecastTrajectory(
        scenario=trajectory.scenario,
        temp_forecast_c=list(trajectory.temp_forecast_c),
        rh_forecast_pct=blended_rh,
        dew_point_forecast_c=blended_dew,
        temp_p25_c=list(trajectory.temp_p25_c),
        temp_p75_c=list(trajectory.temp_p75_c),
        rh_p25_pct=blended_rh_p25,
        rh_p75_pct=blended_rh_p75,
        source="analogue_knn_rh_blend",
        neighbor_count=trajectory.neighbor_count,
        case_count=trajectory.case_count,
        notes=_append_note(trajectory.notes, notes),
    )


# Metadata replacement
# - Purpose: returns an unchanged trajectory payload except for explanatory
#   notes.
# Function purpose: Return the same trajectory with updated explanatory notes.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as trajectory, notes, interpreted according to the
#   implementation below.
# - Outputs: Returns ForecastTrajectory when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _replace_notes(trajectory: ForecastTrajectory, notes: str) -> ForecastTrajectory:
    """Return the same trajectory with updated explanatory notes."""
    return ForecastTrajectory(
        scenario=trajectory.scenario,
        temp_forecast_c=list(trajectory.temp_forecast_c),
        rh_forecast_pct=list(trajectory.rh_forecast_pct),
        dew_point_forecast_c=list(trajectory.dew_point_forecast_c),
        temp_p25_c=list(trajectory.temp_p25_c),
        temp_p75_c=list(trajectory.temp_p75_c),
        rh_p25_pct=list(trajectory.rh_p25_pct),
        rh_p75_pct=list(trajectory.rh_p75_pct),
        source=trajectory.source,
        neighbor_count=trajectory.neighbor_count,
        case_count=trajectory.case_count,
        notes=notes,
    )


# Note concatenation
# - Purpose: merges explanatory note text without repeating the same sentence.
# Function purpose: Append one human-readable note without duplicating it.
# - Project role: Belongs to the forecast model and evaluation layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as existing, addition, interpreted according to the
#   implementation below.
# - Outputs: Returns str when the function completes successfully.
# - Design reason: Forecast-facing code needs explicit documentation because later
#   evaluation, storage, and dashboard layers depend on the exact transformation
#   path.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or evaluation artefacts to gateway orchestration.

def _append_note(existing: str, addition: str) -> str:
    """Append one human-readable note without duplicating it."""
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing}; {addition}"
