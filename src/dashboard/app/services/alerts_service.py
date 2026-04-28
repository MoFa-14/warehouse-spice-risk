# File overview:
# - Responsibility: Services for dashboard alert evaluation and acknowledgement
#   state.
# - Project role: Builds route-ready view models, chart inputs, and interpretive
#   summaries from loaded data.
# - Main data or concerns: View models, chart series, classifications, and
#   display-oriented summaries.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.
# - Why this matters: Keeping presentation logic here prevents routes and templates
#   from reimplementing analysis rules.

"""Services for dashboard alert evaluation and acknowledgement state."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from app.services.thresholds import ClassificationResult, classify_storage_conditions, level_definition
# Class purpose: Alert row shown in the dashboard.
# - Project role: Belongs to the dashboard service and presentation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

@dataclass(frozen=True)
class AlertEntry:
    """Alert row shown in the dashboard."""

    ack_key: str
    pod_id: str
    ts_pc_utc: datetime
    temp_c: float | None
    rh_pct: float | None
    dew_point_c: float | None
    level: int
    level_label: str
    short_label: str
    css_class: str
    color_hex: str
    description: str
    recommendation: str
    acknowledged_until: datetime | None
    acknowledged: bool
# Function purpose: Build active, acknowledged, and banner alert views from latest
#   pod readings.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as latest_readings, acks_file, ack_minutes, now,
#   interpreted according to the rules encoded in the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def build_alert_snapshot(
    latest_readings: Iterable[object],
    acks_file: Path,
    *,
    ack_minutes: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build active, acknowledged, and banner alert views from latest pod readings."""
    current_time = now or datetime.now(timezone.utc)
    acknowledgements = load_acknowledgements(acks_file, now=current_time)

    current_alerts: list[AlertEntry] = []
    for reading in latest_readings:
        alert = reading_to_alert(reading, acknowledgements, now=current_time)
        if alert is not None and alert.level >= 2:
            current_alerts.append(alert)

    current_alerts.sort(key=lambda item: (-item.level, -item.ts_pc_utc.timestamp(), item.pod_id))
    active_alerts = [alert for alert in current_alerts if not alert.acknowledged]
    acknowledged_alerts = [alert for alert in current_alerts if alert.acknowledged]
    banner = build_alert_banner(current_alerts)

    return {
        "all_alerts": current_alerts,
        "active_alerts": active_alerts,
        "acknowledged_alerts": acknowledged_alerts,
        "alert_banner": banner,
        "ack_minutes": ack_minutes,
    }
# Function purpose: Convert one latest pod reading into an alert row when thresholds
#   are exceeded.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as reading, acknowledgements, now, interpreted according
#   to the rules encoded in the body below.
# - Outputs: Returns AlertEntry | None when the function completes successfully.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def reading_to_alert(reading: object, acknowledgements: dict[str, datetime], *, now: datetime) -> AlertEntry | None:
    """Convert one latest pod reading into an alert row when thresholds are exceeded."""
    classification = classify_storage_conditions(reading.temp_c, reading.rh_pct)
    if classification is None:
        return None

    ack_key = build_ack_key(reading.pod_id, classification)
    acknowledged_until = acknowledgements.get(ack_key)
    return AlertEntry(
        ack_key=ack_key,
        pod_id=reading.pod_id,
        ts_pc_utc=reading.ts_pc_utc,
        temp_c=reading.temp_c,
        rh_pct=reading.rh_pct,
        dew_point_c=reading.dew_point_c,
        level=classification.level,
        level_label=classification.level_label,
        short_label=classification.short_label,
        css_class=classification.css_class,
        color_hex=classification.color_hex,
        description=classification.description,
        recommendation=classification.recommendation,
        acknowledged_until=acknowledged_until,
        acknowledged=acknowledged_until is not None and acknowledged_until > now,
    )
# Function purpose: Create a stable acknowledgement key for a pod's current alert
#   condition.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as pod_id, classification, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def build_ack_key(pod_id: str, classification: ClassificationResult) -> str:
    """Create a stable acknowledgement key for a pod's current alert condition."""
    return f"{pod_id}|{classification.level}|{classification.description}"
# Function purpose: Return the top-of-page banner summary.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as alerts, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns dict[str, object] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def build_alert_banner(alerts: list[AlertEntry]) -> dict[str, object]:
    """Return the top-of-page banner summary."""
    if not alerts:
        return {"count": 0, "worst_level": 0, "worst_label": "", "css_class": "", "message": ""}

    worst_level = max(alert.level for alert in alerts)
    definition = level_definition(worst_level)
    count = len(alerts)
    pod_word = "pod" if count == 1 else "pods"
    return {
        "count": count,
        "worst_level": worst_level,
        "worst_label": definition.short_label,
        "css_class": definition.css_class,
        "message": f"{count} {pod_word} currently need attention. Worst severity: {definition.label}.",
    }
# Function purpose: Store a temporary acknowledgement for one alert.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as acks_file, ack_key, minutes, now, interpreted
#   according to the rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def acknowledge_alert(acks_file: Path, ack_key: str, *, minutes: int, now: datetime | None = None) -> None:
    """Store a temporary acknowledgement for one alert."""
    current_time = now or datetime.now(timezone.utc)
    acknowledgements = load_acknowledgements(acks_file, now=current_time)
    acknowledgements[ack_key] = current_time + timedelta(minutes=minutes)
    save_acknowledgements(acks_file, acknowledgements)
# Function purpose: Load acknowledgement expirations from local JSON storage.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as acks_file, now, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns dict[str, datetime] when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def load_acknowledgements(acks_file: Path, *, now: datetime | None = None) -> dict[str, datetime]:
    """Load acknowledgement expirations from local JSON storage."""
    current_time = now or datetime.now(timezone.utc)
    ensure_ack_file(acks_file)
    payload = json.loads(Path(acks_file).read_text(encoding="utf-8"))
    acknowledgements: dict[str, datetime] = {}
    for key, value in payload.items():
        try:
            expiry = datetime.fromisoformat(value)
        except ValueError:
            continue
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        expiry = expiry.astimezone(timezone.utc)
        if expiry > current_time:
            acknowledgements[key] = expiry
    if len(acknowledgements) != len(payload):
        save_acknowledgements(acks_file, acknowledgements)
    return acknowledgements
# Function purpose: Persist acknowledgement expirations to disk.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as acks_file, acknowledgements, interpreted according to
#   the rules encoded in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Persistence-facing code centralizes storage rules so other
#   modules do not duplicate schema or serialization assumptions.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def save_acknowledgements(acks_file: Path, acknowledgements: dict[str, datetime]) -> None:
    """Persist acknowledgement expirations to disk."""
    ensure_ack_file(acks_file)
    serializable = {key: value.astimezone(timezone.utc).isoformat() for key, value in acknowledgements.items()}
    Path(acks_file).write_text(json.dumps(serializable, indent=2), encoding="utf-8")
# Function purpose: Create the acknowledgement storage file when missing.
# - Project role: Belongs to the dashboard service and presentation layer and
#   contributes one focused step within that subsystem.
# - Inputs: Arguments such as acks_file, interpreted according to the rules encoded
#   in the body below.
# - Outputs: No direct return value; the function performs state updates or side
#   effects.
# - Important decisions: Keeping presentation logic here prevents routes and
#   templates from reimplementing analysis rules.
# - Related flow: Consumes dashboard data-access outputs and passes rendered context
#   to routes and templates.

def ensure_ack_file(acks_file: Path) -> None:
    """Create the acknowledgement storage file when missing."""
    path = Path(acks_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("{}", encoding="utf-8")
