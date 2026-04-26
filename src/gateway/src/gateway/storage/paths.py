"""Canonical folder layout for Layer 3 CSV storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


def _repo_root_candidates() -> list[Path]:
    current = Path.cwd().resolve()
    module_path = Path(__file__).resolve()
    candidates: list[Path] = []
    for base in (current, *current.parents, module_path.parent, *module_path.parents):
        if base not in candidates:
            candidates.append(base)
    return candidates


def repo_root() -> Path:
    """Locate the repository root that owns both gateway and firmware."""
    for root in _repo_root_candidates():
        if (root / "gateway" / "pyproject.toml").exists() and (root / "firmware" / "circuitpython-pod").exists():
            return root
    raise FileNotFoundError("Could not locate the repository root for Layer 3 storage paths.")


@dataclass(frozen=True)
class StoragePaths:
    """Resolved filesystem paths for raw, processed, and exported datasets."""

    root: Path

    @property
    def raw_root(self) -> Path:
        return self.root / "raw"

    @property
    def raw_pods_root(self) -> Path:
        return self.raw_root / "pods"

    @property
    def raw_link_quality_root(self) -> Path:
        return self.raw_root / "link_quality"

    @property
    def processed_root(self) -> Path:
        return self.root / "processed"

    @property
    def processed_pods_root(self) -> Path:
        return self.processed_root / "pods"

    @property
    def exports_root(self) -> Path:
        return self.root / "exports"

    @property
    def db_root(self) -> Path:
        return self.root / "db"

    def telemetry_db_path(self) -> Path:
        return self.db_root / "telemetry.sqlite"

    def raw_pod_day_path(self, pod_id: str, day: date) -> Path:
        return self.raw_pods_root / pod_id / f"{day.isoformat()}.csv"

    def raw_link_quality_day_path(self, day: date) -> Path:
        return self.raw_link_quality_root / f"{day.isoformat()}.csv"

    def processed_pod_day_path(self, pod_id: str, day: date) -> Path:
        return self.processed_pods_root / pod_id / f"{day.isoformat()}_processed.csv"

    def training_export_path(self) -> Path:
        return self.exports_root / "training_dataset.csv"

    def ensure_base_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.exports_root.mkdir(parents=True, exist_ok=True)
        self.db_root.mkdir(parents=True, exist_ok=True)


def default_data_root() -> Path:
    """Return the repository-local Layer 3 data root."""
    return repo_root() / "data"


def build_storage_paths(root: Path | str | None = None) -> StoragePaths:
    """Build storage paths, defaulting to the repository data directory."""
    path = Path(root) if root is not None else default_data_root()
    return StoragePaths(path)
