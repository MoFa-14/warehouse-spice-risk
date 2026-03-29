"""Configuration for the Layer 4 Flask dashboard."""

from __future__ import annotations

import os
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


class DashboardConfig:
    """Default dashboard configuration values."""

    DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = Path(__file__).resolve().parents[2]
    DATA_ROOT = SRC_ROOT / "data"
    RAW_ROOT = DATA_ROOT / "raw"
    PROCESSED_ROOT = DATA_ROOT / "processed"
    EXPORTS_ROOT = DATA_ROOT / "exports"
    DB_PATH = DATA_ROOT / "db" / "telemetry.sqlite"
    RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
    ACKS_FILE = RUNTIME_DIR / "acks.json"
    ACK_MINUTES = 30
    MAX_CHART_POINTS = 5000
    DEFAULT_RANGE = "24h"
    AUTO_REFRESH_SECONDS = _env_int("DASHBOARD_AUTO_REFRESH_SECONDS", 0)
    DISPLAY_TIMEZONE = "local"
    SECRET_KEY = "warehouse-spice-risk-dashboard"
