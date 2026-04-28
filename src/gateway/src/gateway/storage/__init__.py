# File overview:
# - Responsibility: Gateway storage package.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Gateway storage package.

Keep package-level imports intentionally light so submodule imports like
`gateway.storage.sqlite_db` do not drag in the CSV writers and create
import-order cycles during startup or tests.
"""

from gateway.storage.paths import StoragePaths, build_storage_paths

__all__ = ["StoragePaths", "build_storage_paths"]
