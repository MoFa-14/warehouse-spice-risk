"""Analogue / kNN trajectory forecasting."""

from __future__ import annotations

from math import sqrt

from forecasting.config import ForecastConfig
from forecasting.dewpoint import calculate_dew_point_c
from forecasting.models import CaseRecord, FeatureVector, ForecastTrajectory, TimeSeriesPoint
from forecasting.utils import clamp, mean, percentile, population_std


class AnalogueKNNForecaster:
    """Forecast future temperature and RH using stored analogue cases."""

    def __init__(self, *, config: ForecastConfig) -> None:
        self.config = config

    def forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        cases: list[CaseRecord],
    ) -> ForecastTrajectory:
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

        means, stds = _feature_stats(usable_cases)
        scored_cases = sorted(
            (
                _distance(
                    current=feature_vector.values,
                    candidate=case.feature_vector,
                    means=means,
                    stds=stds,
                    weights=self.config.feature_weights,
                ),
                case,
            )
            for case in usable_cases
        )
        neighbors = [case for _, case in scored_cases[: min(self.config.knn_k, len(scored_cases))]]
        return _aggregate_neighbors(neighbors, scenario="baseline")

    def _fallback_forecast(
        self,
        *,
        feature_vector: FeatureVector,
        baseline_window: list[TimeSeriesPoint],
        case_count: int,
    ) -> ForecastTrajectory:
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
    total = 0.0
    for key, current_value in current.items():
        if key not in candidate or key not in means or key not in stds:
            continue
        weight = weights.get(key, 1.0)
        normalized_current = (float(current_value) - means[key]) / stds[key]
        normalized_candidate = (float(candidate[key]) - means[key]) / stds[key]
        total += weight * (normalized_current - normalized_candidate) ** 2
    return sqrt(total)


def _aggregate_neighbors(neighbors: list[CaseRecord], *, scenario: str) -> ForecastTrajectory:
    horizon = min(len(case.future_temp_c) for case in neighbors)
    temp_forecast: list[float] = []
    rh_forecast: list[float] = []
    dew_forecast: list[float] = []
    temp_p25: list[float] = []
    temp_p75: list[float] = []
    rh_p25: list[float] = []
    rh_p75: list[float] = []

    for step in range(horizon):
        temp_values = [case.future_temp_c[step] for case in neighbors]
        rh_values = [case.future_rh_pct[step] for case in neighbors]
        temp_mid = percentile(temp_values, 50.0)
        rh_mid = percentile(rh_values, 50.0)
        temp_forecast.append(temp_mid)
        rh_forecast.append(rh_mid)
        dew_forecast.append(calculate_dew_point_c(temp_mid, rh_mid))
        temp_p25.append(percentile(temp_values, 25.0))
        temp_p75.append(percentile(temp_values, 75.0))
        rh_p25.append(percentile(rh_values, 25.0))
        rh_p75.append(percentile(rh_values, 75.0))

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
        notes=f"Median forecast over {len(neighbors)} nearest historical cases.",
    )
