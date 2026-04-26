"""UTC time helpers shared by logging, storage, and preprocessing."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso(moment: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp with a trailing Z."""
    value = moment or utc_now()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to timezone-aware UTC."""
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)
