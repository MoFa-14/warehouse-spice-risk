# File overview:
# - Responsibility: UTC time helpers shared by logging, storage, and preprocessing.
# - Project role: Provides reusable low-level helpers for timing, retry logic, and
#   sequence handling.
# - Main data or concerns: Helper arguments, timestamps, counters, and shared return
#   values.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.
# - Why this matters: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.

"""UTC time helpers shared by logging, storage, and preprocessing."""

from __future__ import annotations

from datetime import datetime, timezone
# Function purpose: Return the current timezone-aware UTC datetime.
# - Project role: Belongs to the shared gateway utility layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.

def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)
# Function purpose: Return an ISO-8601 UTC timestamp with a trailing Z.
# - Project role: Belongs to the shared gateway utility layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as moment, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns str when the function completes successfully.
# - Important decisions: Keeping small utility rules centralized prevents subtle
#   duplication across transport and storage code.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.

def utc_now_iso(moment: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp with a trailing Z."""
    value = moment or utc_now()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
# Function purpose: Parse an ISO-8601 timestamp and normalize it to timezone-aware
#   UTC.
# - Project role: Belongs to the shared gateway utility layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as value, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns datetime when the function completes successfully.
# - Important decisions: Parsing and validation code must make acceptance rules
#   explicit because later storage and forecasting logic assume normalized payloads.
# - Related flow: Supports higher-level gateway modules with focused helper
#   behavior.

def parse_utc_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to timezone-aware UTC."""
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)
