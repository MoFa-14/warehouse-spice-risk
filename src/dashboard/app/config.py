# File overview:
# - Responsibility: Configuration for the Layer 4 Flask dashboard.
# - Project role: Defines app configuration, initialization, route setup, and
#   dashboard-wide utilities.
# - Main data or concerns: Configuration values, route parameters, and app-level
#   helper state.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.
# - Why this matters: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.

"""Configuration for the Layer 4 Flask dashboard."""

from __future__ import annotations

import os
from pathlib import Path
# Function purpose: Implements the env int step used by this subsystem.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as name, default, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns int when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default
# Function purpose: Implements the env path step used by this subsystem.
# - Project role: Belongs to the dashboard application wiring layer and contributes
#   one focused step within that subsystem.
# - Inputs: Arguments such as name, default, interpreted according to the rules
#   encoded in the body below.
# - Outputs: Returns Path when the function completes successfully.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return Path(value).expanduser()
# Class purpose: Default dashboard configuration values.
# - Project role: Belongs to the dashboard application wiring layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: App wiring needs to stay compact and explicit because every
#   dashboard page depends on it.
# - Related flow: Coordinates dashboard services, configuration, and Flask entry
#   points.

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
    TELEMETRY_ADJUSTMENTS_PATH = _env_path(
        "DSP_TELEMETRY_ADJUSTMENTS_PATH",
        DATA_ROOT / "config" / "telemetry_adjustments.json",
    )
    SECRET_KEY = "warehouse-spice-risk-dashboard"
