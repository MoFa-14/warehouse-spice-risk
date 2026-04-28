# File overview:
# - Responsibility: Canonical folder layout for Layer 3 CSV storage.
# - Project role: Stores raw telemetry, link diagnostics, and exportable datasets in
#   canonical formats.
# - Main data or concerns: SQLite rows, CSV rows, schema definitions, and storage
#   paths.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.
# - Why this matters: Persistence code matters because the rest of the project only
#   sees what this layer records and exposes.

"""Canonical folder layout for Layer 3 CSV storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
# Function purpose: Implements the repo root candidates step used by this subsystem.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns list[Path] when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def _repo_root_candidates() -> list[Path]:
    current = Path.cwd().resolve()
    module_path = Path(__file__).resolve()
    candidates: list[Path] = []
    for base in (current, *current.parents, module_path.parent, *module_path.parents):
        if base not in candidates:
            candidates.append(base)
    return candidates
# Function purpose: Locate the repository root that owns both gateway and firmware.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns Path when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def repo_root() -> Path:
    """Locate the repository root that owns both gateway and firmware."""
    for root in _repo_root_candidates():
        if (root / "gateway" / "pyproject.toml").exists() and (root / "firmware" / "circuitpython-pod").exists():
            return root
    raise FileNotFoundError("Could not locate the repository root for Layer 3 storage paths.")
# Class purpose: Resolved filesystem paths for raw, processed, and exported
#   datasets.
# - Project role: Belongs to the gateway persistence layer and groups related state
#   or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

@dataclass(frozen=True)
class StoragePaths:
    """Resolved filesystem paths for raw, processed, and exported datasets."""

    root: Path
    # Method purpose: Implements the raw root step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def raw_root(self) -> Path:
        return self.root / "raw"
    # Method purpose: Implements the raw pods root step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def raw_pods_root(self) -> Path:
        return self.raw_root / "pods"
    # Method purpose: Implements the raw link quality root step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def raw_link_quality_root(self) -> Path:
        return self.raw_root / "link_quality"
    # Method purpose: Implements the processed root step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def processed_root(self) -> Path:
        return self.root / "processed"
    # Method purpose: Implements the processed pods root step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def processed_pods_root(self) -> Path:
        return self.processed_root / "pods"
    # Method purpose: Implements the exports root step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def exports_root(self) -> Path:
        return self.root / "exports"
    # Method purpose: Implements the database root step used by this subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    @property
    def db_root(self) -> Path:
        return self.root / "db"
    # Method purpose: Implements the telemetry database path step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def telemetry_db_path(self) -> Path:
        return self.db_root / "telemetry.sqlite"
    # Method purpose: Implements the raw pod day path step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: Arguments such as pod_id, day, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def raw_pod_day_path(self, pod_id: str, day: date) -> Path:
        return self.raw_pods_root / pod_id / f"{day.isoformat()}.csv"
    # Method purpose: Implements the raw link quality day path step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: Arguments such as day, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def raw_link_quality_day_path(self, day: date) -> Path:
        return self.raw_link_quality_root / f"{day.isoformat()}.csv"
    # Method purpose: Implements the processed pod day path step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: Arguments such as pod_id, day, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def processed_pod_day_path(self, pod_id: str, day: date) -> Path:
        return self.processed_pods_root / pod_id / f"{day.isoformat()}_processed.csv"
    # Method purpose: Implements the training export path step used by this
    #   subsystem.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns Path when the function completes successfully.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def training_export_path(self) -> Path:
        return self.exports_root / "training_dataset.csv"
    # Method purpose: Ensures that base dirs exists before later logic depends
    #   on it.
    # - Project role: Belongs to the gateway persistence layer and acts as a
    #   method on StoragePaths.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Persistence code matters because the rest of the
    #   project only sees what this layer records and exposes.
    # - Related flow: Receives normalized gateway records and passes stored
    #   evidence to forecasting and dashboard loaders.

    def ensure_base_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.exports_root.mkdir(parents=True, exist_ok=True)
        self.db_root.mkdir(parents=True, exist_ok=True)
# Function purpose: Return the repository-local Layer 3 data root.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: No explicit arguments beyond module or instance context.
# - Outputs: Returns Path when the function completes successfully.
# - Important decisions: Persistence code matters because the rest of the project
#   only sees what this layer records and exposes.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def default_data_root() -> Path:
    """Return the repository-local Layer 3 data root."""
    return repo_root() / "data"
# Function purpose: Build storage paths, defaulting to the repository data
#   directory.
# - Project role: Belongs to the gateway persistence layer and contributes one
#   focused step within that subsystem.
# - Inputs: Arguments such as root, interpreted according to the rules encoded in
#   the body below.
# - Outputs: Returns StoragePaths when the function completes successfully.
# - Important decisions: The transformation rules here define how later code
#   interprets the same data, so the shape of the output needs to stay stable and
#   reproducible.
# - Related flow: Receives normalized gateway records and passes stored evidence to
#   forecasting and dashboard loaders.

def build_storage_paths(root: Path | str | None = None) -> StoragePaths:
    """Build storage paths, defaulting to the repository data directory."""
    path = Path(root) if root is not None else default_data_root()
    return StoragePaths(path)
