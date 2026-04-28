# File overview:
# - Responsibility: Data-access exports for the Flask dashboard.
# - Project role: Loads persisted telemetry, forecast, or evaluation data into
#   stable dashboard-facing tables.
# - Main data or concerns: Telemetry rows, forecast rows, evaluation rows, and date
#   or pod filters.
# - Related flow: Reads stored files or database rows and passes normalized frames
#   to dashboard services.
# - Why this matters: Dashboard services depend on these loaders to keep storage
#   assumptions centralized.

"""Data-access exports for the Flask dashboard."""

from app.data_access.csv_reader import read_link_quality, read_processed_samples, read_raw_samples
from app.data_access.file_finder import (
    discover_pod_ids,
    find_link_quality_files,
    find_processed_pod_files,
    find_raw_pod_files,
    latest_file,
)
from app.data_access.forecast_reader import read_latest_forecasts
from app.data_access.sqlite_reader import (
    discover_pod_ids_from_sqlite,
    read_link_quality_sqlite,
    read_raw_samples_sqlite,
    sqlite_db_exists,
)

__all__ = [
    "discover_pod_ids",
    "discover_pod_ids_from_sqlite",
    "find_link_quality_files",
    "find_processed_pod_files",
    "find_raw_pod_files",
    "latest_file",
    "read_latest_forecasts",
    "read_link_quality",
    "read_link_quality_sqlite",
    "read_processed_samples",
    "read_raw_samples",
    "read_raw_samples_sqlite",
    "sqlite_db_exists",
]
