"""Rule-based storage thresholds and severity classification."""

from __future__ import annotations

import math
from dataclasses import dataclass


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


def classify_storage_conditions(temp_c: float | None, rh_pct: float | None) -> ClassificationResult | None:
    """Classify one reading using the project alert rules."""
    if _is_missing(temp_c) or _is_missing(rh_pct):
        return None

    triggered: list[tuple[int, str]] = []

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


def threshold_legend() -> list[dict[str, str]]:
    """Return threshold descriptions for the pod detail legend."""
    return [
        {
            "metric": "Temperature",
            "range": f"Optimal {T_OPT_MIN:.0f}-{T_OPT_MAX:.0f} C | Warning > {T_WARN_HIGH:.0f} C | Critical >= {T_CRIT_HIGH:.0f} C",
            "note": "Project rule: 25 C is treated as critical heat for fast degradation.",
        },
        {
            "metric": "Relative Humidity",
            "range": f"Ideal {RH_IDEAL_MIN:.0f}-{RH_IDEAL_MAX:.0f}% | Warning > {RH_WARN_HIGH:.0f}% | High Risk > {RH_HIGH_RISK:.0f}% | Critical >= {RH_MOLD_CRIT:.0f}%",
            "note": "Below 30% is also a warning because spices can dry out and lose volatile flavor compounds.",
        },
    ]


def level_definition(level: int) -> AlertLevelDefinition:
    """Return metadata for a numeric alert level."""
    return LEVEL_DEFINITIONS[level]


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))
