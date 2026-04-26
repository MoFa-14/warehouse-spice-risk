"""Analogue / kNN trajectory forecasting.

This file contains the core baseline forecaster used by the project.

Conceptually, the model asks:
"Given what the last 3 hours looked like, which historical cases looked most
similar, and what happened in the next 30 minutes after those cases?"

That makes this file one of the most viva-relevant parts of the codebase. It is
where the system moves from a descriptive feature vector to an actual forward
trajectory for temperature, RH, and derived dew point.
"""

from __future__ import annotations

from datetime import datetime
from math import sqrt

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import CaseRecord, FeatureVector, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import clamp, mean, parse_utc, percentile, population_std


class AnalogueKNNForecaster:
    """Forecast future temperature and RH using stored analogue cases.

    The model is intentionally interpretable:
    - it compares the current feature vector with historical cases
    - it selects the nearest neighbours
    - it aggregates the neighbours' future behaviour into a median trajectory

    This makes it easier to justify in a dissertation than a black-box model,
    while still supporting multi-variable, multi-step forecasts.
    """

    def __init__(self, *, config: ForecastConfig) -> None:
        self.config = config

    def forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        cases: list[CaseRecord],
    ) -> ForecastTrajectory:
        """Build the baseline forecast for one pod and one timestamp.

        This method is called by the forecast runner after the gateway has:
        - read the latest 3-hour history window
        - run event detection
        - prepared the baseline-safe window
        - extracted the feature vector

        The method then decides whether there is enough trustworthy historical
        support to use analogue forecasting. If not, it falls back to a simpler
        bounded continuation strategy.
        """
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

        # We normalise feature distances using statistics from the available
        # case pool so no single feature dominates only because it has a larger
        # numeric scale.
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
        # RH/dew regime gating is a deliberately interpretable safeguard added
        # after observing that some forecasts were matching to obviously wrong
        # humidity regimes.
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

        # A small recency penalty prefers newer cases when similarity is
        # otherwise similar. This is a lightweight way to introduce some
        # time-awareness without redesigning the model into a seasonal learner.
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
        # Even after gating, the model may have only a thin set of acceptable RH
        # analogues. In that situation we keep the temperature trajectory from
        # the analogue model, but pull RH part-way back toward persistence so the
        # model does not overclaim confidence.
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

    def _fallback_forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        case_count: int,
    ) -> ForecastTrajectory:
        """Return a bounded slope-persistence fallback when case support is weak.

        This fallback is intentionally simple enough to explain under pressure:
        it starts from the latest observed point, continues the recent slope with
        rate caps, and widens an uncertainty band with forecast horizon.
        """
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
            # Dew point is always recomputed from the evolving temperature/RH
            # pair so it remains physically consistent with the fallback path.
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


def _distance(
    *,
    current: dict[str, float],
    candidate: dict[str, float],
    means: dict[str, float],
    stds: dict[str, float],
    weights: dict[str, float],
) -> float:
    """Compute weighted Euclidean distance in normalised feature space.

    In plain language, this is the similarity score that tells the analogue
    model which historical cases deserve attention.
    """
    total = 0.0
    for key, current_value in current.items():
        if key not in candidate or key not in means or key not in stds:
            continue
        weight = weights.get(key, 1.0)
        normalized_current = (float(current_value) - means[key]) / stds[key]
        normalized_candidate = (float(candidate[key]) - means[key]) / stds[key]
        total += weight * (normalized_current - normalized_candidate) ** 2
    return sqrt(total)


def _is_rh_regime_compatible(
    *,
    current: dict[str, float],
    candidate: dict[str, float],
    config: ForecastConfig,
) -> bool:
    """Reject cases that live in a clearly different humidity regime.

    This gate was added after finding that visually "closest" cases could still
    come from very different RH/dew conditions and pull the forecast into the
    wrong regime.
    """
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


def _recency_penalty(
    *,
    current_ts: datetime,
    candidate_ts: datetime,
    config: ForecastConfig,
) -> float:
    """Add a small age penalty so newer cases are preferred when similarity is close."""
    age_days = max((current_ts - candidate_ts).total_seconds() / 86400.0, 0.0)
    return min(age_days * config.analogue_recency_penalty_per_day, config.analogue_recency_penalty_cap)


def _rh_support_weight(*, neighbor_count: int, minimum_case_count: int) -> float:
    """Convert analogue support into a smooth RH blending weight.

    The intent is not to discard analogue information completely when support is
    merely thin. Instead, it expresses confidence as a number between full
    persistence and full analogue RH.
    """
    if minimum_case_count <= 1:
        return 1.0 if neighbor_count > 0 else 0.0
    missing_support = max(minimum_case_count - neighbor_count, 0)
    return clamp(1.0 - (missing_support / float(minimum_case_count)) ** 2, 0.0, 1.0)


def _weak_rh_support_threshold(minimum_case_count: int) -> int:
    """Define when RH analogue support is weak enough to justify blending."""
    if minimum_case_count <= 1:
        return 1
    return max(2, minimum_case_count - 1)


def _aggregate_neighbors(
    neighbors: list[CaseRecord],
    *,
    scenario: str,
    anchor_temp_c: float,
    anchor_rh_pct: float,
) -> ForecastTrajectory:
    """Aggregate neighbour futures into one baseline forecast trajectory.

    The project now uses *re-anchored offsets* rather than raw absolute future
    values. That means each neighbour contributes the shape of its future change
    relative to its own anchor, and the median of those offsets is then applied
    to the current live anchor.

    This was a key fix: it prevents the forecast from jumping instantly toward a
    neighbour's absolute level when that neighbour lived in a different regime.
    """
    horizon = min(len(case.future_temp_c) for case in neighbors)
    temp_forecast: list[float] = []
    rh_forecast: list[float] = []
    dew_forecast: list[float] = []
    temp_p25: list[float] = []
    temp_p75: list[float] = []
    rh_p25: list[float] = []
    rh_p75: list[float] = []

    for step in range(horizon):
        # The forecaster works in offsets from each neighbour's own last point.
        # In viva language: we reuse the neighbour's *pattern of change*, not
        # the neighbour's absolute level.
        temp_offsets = [
            float(case.future_temp_c[step]) - float(case.feature_vector.get("temp_last", case.future_temp_c[0]))
            for case in neighbors
        ]
        rh_offsets = [
            float(case.future_rh_pct[step]) - float(case.feature_vector.get("rh_last", case.future_rh_pct[0]))
            for case in neighbors
        ]
        # Median aggregation keeps the trajectory more robust than a mean when
        # one historical case has an odd future path.
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


def _blend_rh_with_persistence(
    *,
    trajectory: ForecastTrajectory,
    baseline_window: list[TimeSeriesPoint],
    analogue_weight: float,
    notes: str,
) -> ForecastTrajectory:
    """Blend analogue RH toward persistence while leaving the temperature path intact.

    This is a small targeted safeguard. It acknowledges that the analogue model
    may still have useful temperature guidance even when humidity support is too
    thin to trust fully.
    """
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


def _append_note(existing: str, addition: str) -> str:
    """Append one human-readable note without duplicating it."""
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing}; {addition}"
