"""Gateway storage package.

Keep package-level imports intentionally light so submodule imports like
`gateway.storage.sqlite_db` do not drag in the CSV writers and create
import-order cycles during startup or tests.
"""

from gateway.storage.paths import StoragePaths, build_storage_paths

__all__ = ["StoragePaths", "build_storage_paths"]
