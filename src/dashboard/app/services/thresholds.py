# File overview:
# - Responsibility: Rule-based storage thresholds and severity classification.
# - Project role: Builds route-ready view models, chart inputs, and interpretive
#   summaries from loaded data.
# - Main data or concerns: View models, chart series, classifications, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.
# - Why this matters: Keeping presentation logic here prevents routes and templates
#   from reimplementing analysis rules.

"""Rule-based storage thresholds and severity classification.

This module defines the storage-condition interpretation language used across
the dashboard. The thresholds here are not generic environmental comfort bands.
They are project-specific rules intended to express warehouse spice risk in a
clear, explainable way.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


T_OPT_MIN = 10.0
T_OPT_MAX = 21.0
T_WARN_HIGH = 22.0
T_CRIT_HIGH = 25.0

RH_LOW = 30.0
RH_IDEAL_MIN = 30.0
RH_IDEAL_MAX = 50.0
RH_WARN_HIGH = 50.0
RH_HIGH_RISK = 60.0
RH_MOLD_CRIT = 65.0
# Class purpose: Metadata displayed for one alert level.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class AlertLevelDefinition:
    """Metadata displayed for one alert level."""

    level: int
    label: str
    short_label: str
    css_class: str
    color_hex: str
    description: str
    recommendation: str
# Class purpose: Result of classifying one temperature/humidity pair.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class ClassificationResult:
    """Result of classifying one temperature/humidity pair."""

    level: int
    level_label: str
    short_label: str
    css_class: str
    color_hex: str
    description: str
    recommendation: str
    reasons: tuple[str, ...]
# Class purpose: Worst predicted classification found across a forecast trajectory.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class TrajectoryClassificationResult:
    """Worst predicted classification found across a forecast trajectory."""

    status: ClassificationResult
    horizon_minute: int
    temp_c: float
    rh_pct: float


LEVEL_DEFINITIONS = {
    0: AlertLevelDefinition(
        level=0,
        label="OPTIMAL",
        short_label="Optimal",
        css_class="level-optimal",
        color_hex="#2f855a",
        description="Within optimal storage ranges.",
        recommendation="Maintain current storage conditions.",
    ),
    1: AlertLevelDefinition(
        level=1,
        label="GOOD/ACCEPTABLE",
        short_label="Good",
        css_class="level-good",
        color_hex="#78c850",
        description="Safe but slightly outside the ideal storage window.",
        recommendation="No urgent action needed; keep conditions stable.",
    ),
    2: AlertLevelDefinition(
        level=2,
        label="WARNING",
        short_label="Warning",
        css_class="level-warning",
        color_hex="#d69e2e",
        description="Conditions likely to harm quality if sustained.",
        recommendation="Monitor closely; improve ventilation/dehumidification if trend continues.",
    ),
    3: AlertLevelDefinition(
        level=3,
        label="HIGH RISK",
        short_label="High Risk",
        css_class="level-high-risk",
        color_hex="#dd6b20",
        description="Strong risk of caking and quality loss.",
        recommendation="Take action: dehumidify/ventilate; inspect stock; consider relocation.",
    ),
    4: AlertLevelDefinition(
        level=4,
        label="CRITICAL",
        short_label="Critical",
        css_class="level-critical",
        color_hex="#c53030",
        description="Rapid spoilage or mold risk is present.",
        recommendation="Immediate action required: isolate/relocate sensitive stock; reduce humidity/temperature; inspect for mold.",
    ),
}
# Function purpose: Classify one temperature/humidity pair into a storage-risk
#   level.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as temp_c, rh_pct, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns ClassificationResult | None when the function completes
#   successfully.
# - Important decisions: The implementation encodes a project decision point that
#   later evaluation, storage, or dashboard logic depends on directly.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def classify_storage_conditions(temp_c: float | None, rh_pct: float | None) -> ClassificationResult | None:
    """Classify one temperature/humidity pair into a storage-risk level.

    The dashboard uses this function in several contexts:

    - latest pod state cards,
    - alert generation,
    - forecast trajectory interpretation,
    - and review summaries.

    It therefore acts as the shared vocabulary for saying whether a condition
    is optimal, acceptable, warning-level, high-risk, or critical.
    """
    if _is_missing(temp_c) or _is_missing(rh_pct):
        return None

    triggered: list[tuple[int, str]] = []

    # Humidity thresholds are treated as especially important because excessive
    # moisture drives clumping, caking, volatile-oil loss, and mold risk in the
    # warehouse spice use case.
    if rh_pct >= RH_MOLD_CRIT:
        triggered.append((4, "Rapid mold growth risk"))
    elif rh_pct > RH_HIGH_RISK:
        triggered.append((3, "High humidity: caking/lumping, volatile oils loss, mold risk rising"))
    elif rh_pct > RH_WARN_HIGH:
        triggered.append((2, "Humidity above ideal: clumping risk increasing"))
    elif rh_pct < RH_LOW:
        triggered.append((2, "Too dry: may lose volatile flavor compounds"))

    if temp_c >= T_CRIT_HIGH:
        triggered.append((4, "Severe heat: rapid aroma/color degradation"))
    elif temp_c > T_WARN_HIGH:
        triggered.append((2, "Warm storage: quality loss accelerates"))

    if T_OPT_MIN <= temp_c <= T_OPT_MAX and RH_IDEAL_MIN <= rh_pct <= RH_IDEAL_MAX:
        definition = LEVEL_DEFINITIONS[0]
        return ClassificationResult(
            level=definition.level,
            level_label=definition.label,
            short_label=definition.short_label,
            css_class=definition.css_class,
            color_hex=definition.color_hex,
            description=definition.description,
            recommendation=definition.recommendation,
            reasons=(definition.description,),
        )

    if not triggered:
        definition = LEVEL_DEFINITIONS[1]
        return ClassificationResult(
            level=definition.level,
            level_label=definition.label,
            short_label=definition.short_label,
            css_class=definition.css_class,
            color_hex=definition.color_hex,
            description=definition.description,
            recommendation=definition.recommendation,
            reasons=(definition.description,),
        )

    max_level = max(level for level, _ in triggered)
    reasons = tuple(dict.fromkeys(message for level, message in triggered if level == max_level))
    definition = LEVEL_DEFINITIONS[max_level]
    return ClassificationResult(
        level=definition.level,
        level_label=definition.label,
        short_label=definition.short_label,
        css_class=definition.css_class,
        color_hex=definition.color_hex,
        description="; ".join(reasons),
        recommendation=definition.recommendation,
        reasons=reasons,
    )
# Function purpose: Return the worst predicted classification across a forecast
#   horizon.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as temp_forecast_c, rh_forecast_pct, interpreted
#   according to the rules encoded in the body below.
# - Outputs: Returns TrajectoryClassificationResult | None when the function
#   completes successfully.
# - Important decisions: The implementation encodes a project decision point that
#   later evaluation, storage, or dashboard logic depends on directly.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def classify_storage_trajectory(
    temp_forecast_c: Sequence[float | None],
    rh_forecast_pct: Sequence[float | None],
) -> TrajectoryClassificationResult | None:
    """Return the worst predicted classification across a forecast horizon.

    The forecast page uses this result to summarise the most concerning point of
    a predicted trajectory rather than forcing the user to inspect every minute
    of the curve manually.
    """
    worst: TrajectoryClassificationResult | None = None
    for minute_index, (temp_c, rh_pct) in enumerate(zip(temp_forecast_c, rh_forecast_pct), start=1):
        status = classify_storage_conditions(temp_c, rh_pct)
        if status is None:
            continue
        if worst is None or status.level > worst.status.level:
            worst = TrajectoryClassificationResult(
                status=status,
                horizon_minute=minute_index,
                temp_c=float(temp_c),
                rh_pct=float(rh_pct),
            )
    return worst
# Function purpose: Return the metadata used by the threshold-legend user interface.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns list[dict[str, object]] when the function completes
#   successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def threshold_legend() -> list[dict[str, object]]:
    """Return the metadata used by the threshold-legend user interface."""
    return [
        {
            "metric": "Temperature",
            "theme": "temp",
            "subtitle": "Heat stress thresholds used for live status and forecast alerts.",
            "range": f"Optimal {T_OPT_MIN:.0f}-{T_OPT_MAX:.0f} C | Warning > {T_WARN_HIGH:.0f} C | Critical >= {T_CRIT_HIGH:.0f} C",
            "note": "Project rule: 25 C is treated as critical heat for fast degradation.",
            "bands": [
                {"label": "Optimal", "value": f"{T_OPT_MIN:.0f}-{T_OPT_MAX:.0f} C", "tone": "optimal"},
                {"label": "Warning", "value": f"> {T_WARN_HIGH:.0f} C", "tone": "warning"},
                {"label": "Critical", "value": f">= {T_CRIT_HIGH:.0f} C", "tone": "critical"},
            ],
        },
        {
            "metric": "Relative Humidity",
            "theme": "rh",
            "subtitle": "Moisture thresholds used for clumping, caking, and mold warnings.",
            "range": f"Ideal {RH_IDEAL_MIN:.0f}-{RH_IDEAL_MAX:.0f}% | Warning > {RH_WARN_HIGH:.0f}% | High Risk > {RH_HIGH_RISK:.0f}% | Critical >= {RH_MOLD_CRIT:.0f}%",
            "note": "Below 30% is also a warning because spices can dry out and lose volatile flavor compounds.",
            "bands": [
                {"label": "Ideal", "value": f"{RH_IDEAL_MIN:.0f}-{RH_IDEAL_MAX:.0f}%", "tone": "optimal"},
                {"label": "Warning", "value": f"> {RH_WARN_HIGH:.0f}%", "tone": "warning"},
                {"label": "High Risk", "value": f"> {RH_HIGH_RISK:.0f}%", "tone": "high-risk"},
                {"label": "Critical", "value": f">= {RH_MOLD_CRIT:.0f}%", "tone": "critical"},
            ],
        },
    ]
# Function purpose: Return metadata for a numeric alert level.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as level, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns AlertLevelDefinition when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def level_definition(level: int) -> AlertLevelDefinition:
    """Return metadata for a numeric alert level."""
    return LEVEL_DEFINITIONS[level]
# Function purpose: Implements the is missing step used by this subsystem.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns bool when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))
