# File overview:
# - Responsibility: Timezone helpers for dashboard display and local-input parsing.
# - Project role: Defines app configuration, initialization, route setup, and
#   dashboard-wide utilities.
# - Main data or concerns: Configuration values, route parameters, and app-level
#   helper state.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.
# - Why this matters: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.

"""Timezone helpers for dashboard display and local-input parsing."""

from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc
# Function purpose: Resolve the dashboard display timezone from config, defaulting
#   to local system time.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as configured, interpreted according to the rules encoded
#   in the body below.
# - Outputs: Returns tzinfo when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def resolve_display_timezone(configured: object | None) -> tzinfo:
    """Resolve the dashboard display timezone from config, defaulting to local system time."""
    if isinstance(configured, tzinfo):
        return configured

    text = str(configured or "").strip()
    if not text or text.lower() == "local":
        return _local_timezone()
    if text.upper() == "UTC":
        return UTC

    try:
        return ZoneInfo(text)
    except ZoneInfoNotFoundError:
        return _local_timezone()
# Function purpose: Convert a timestamp to the configured display timezone.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, display_timezone, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def to_display_time(value: datetime, display_timezone: tzinfo) -> datetime:
    """Convert a timestamp to the configured display timezone."""
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.astimezone(display_timezone)
# Function purpose: Format a dashboard timestamp using the configured display
#   timezone.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, display_timezone, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def format_display_timestamp(value: datetime | None, display_timezone: tzinfo) -> str:
    """Format a dashboard timestamp using the configured display timezone."""
    if value is None:
        return "No data"
    local_value = to_display_time(value, display_timezone)
    return local_value.strftime("%Y-%m-%d %H:%M:%S %Z")
# Function purpose: Format a timestamp for a datetime-local input in the display
#   timezone.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, display_timezone, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def format_datetime_local_value(value: datetime, display_timezone: tzinfo) -> str:
    """Format a timestamp for a datetime-local input in the display timezone."""
    return to_display_time(value, display_timezone).strftime("%Y-%m-%dT%H:%M")
# Function purpose: Parse a datetime-local input value as display-local time and
#   convert to UTC.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as value, display_timezone, interpreted according to the
#   rules encoded in the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def parse_datetime_local_input(value: str, display_timezone: tzinfo) -> datetime:
    """Parse a datetime-local input value as display-local time and convert to UTC."""
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=display_timezone)
    return moment.astimezone(UTC)
# Function purpose: Return a short human-friendly label for the active display
#   timezone.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as display_timezone, reference, interpreted according to
#   the rules encoded in the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def timezone_label(display_timezone: tzinfo, reference: datetime | None = None) -> str:
    """Return a short human-friendly label for the active display timezone."""
    instant = reference or datetime.now(UTC)
    label = to_display_time(instant, display_timezone).tzname()
    return label or "Local time"
# Function purpose: Implements the local timezone step used by this subsystem.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns tzinfo when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _local_timezone() -> tzinfo:
    local = datetime.now(UTC).astimezone().tzinfo
    return local or UTC
