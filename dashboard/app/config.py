"""Configuration for the Layer 4 Flask dashboard."""

from __future__ import annotations

from pathlib import Path


class DashboardConfig:
    """Default dashboard configuration values."""

    DASHBOARD_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = Path(__file__).resolve().parents[2]
    DATA_ROOT = SRC_ROOT / "data"
    RAW_ROOT = DATA_ROOT / "raw"
    PROCESSED_ROOT = DATA_ROOT / "processed"
    EXPORTS_ROOT = DATA_ROOT / "exports"
    RUNTIME_DIR = Path(__file__).resolve().parent / "runtime"
    ACKS_FILE = RUNTIME_DIR / "acks.json"
    ACK_MINUTES = 30
    MAX_CHART_POINTS = 5000
    DEFAULT_RANGE = "24h"
    SECRET_KEY = "warehouse-spice-risk-dashboard"
