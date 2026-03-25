"""File-discovery helpers for dashboard CSV access."""

from __future__ import annotations

from datetime import date
from pathlib import Path


def discover_pod_ids(data_root: Path) -> list[str]:
    """Return pod ids discovered under data/raw/pods."""
    pods_root = Path(data_root) / "raw" / "pods"
    if not pods_root.exists():
        return []
    return sorted(path.name for path in pods_root.iterdir() if path.is_dir())


def find_raw_pod_files(
    data_root: Path,
    pod_id: str,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[Path]:
    """Return raw per-day CSV files for a pod inside a date range."""
    pod_root = Path(data_root) / "raw" / "pods" / pod_id
    return _find_dated_files(pod_root, date_from=date_from, date_to=date_to)


def find_processed_pod_files(
    data_root: Path,
    pod_id: str,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[Path]:
    """Return processed per-day CSV files for a pod inside a date range."""
    pod_root = Path(data_root) / "processed" / "pods" / pod_id
    return _find_dated_files(
        pod_root,
        pattern="*_processed.csv",
        date_from=date_from,
        date_to=date_to,
        stem_parser=_parse_processed_stem,
    )


def find_link_quality_files(
    data_root: Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[Path]:
    """Return link-quality daily CSV files inside a date range."""
    link_root = Path(data_root) / "raw" / "link_quality"
    return _find_dated_files(link_root, date_from=date_from, date_to=date_to)


def latest_file(paths: list[Path]) -> Path | None:
    """Return the most recent dated CSV path from a sorted list."""
    if not paths:
        return None
    return paths[-1]


def _find_dated_files(
    root: Path,
    *,
    pattern: str = "*.csv",
    date_from: date | None = None,
    date_to: date | None = None,
    stem_parser=None,
) -> list[Path]:
    if not root.exists():
        return []

    parser = stem_parser or _parse_date_stem
    files: list[tuple[date, Path]] = []
    for path in root.glob(pattern):
        if not path.is_file():
            continue
        try:
            file_date = parser(path.stem)
        except ValueError:
            continue
        if date_from is not None and file_date < date_from:
            continue
        if date_to is not None and file_date > date_to:
            continue
        files.append((file_date, path))
    files.sort(key=lambda item: (item[0], item[1].name))
    return [path for _, path in files]


def _parse_date_stem(stem: str) -> date:
    return date.fromisoformat(stem)


def _parse_processed_stem(stem: str) -> date:
    return date.fromisoformat(stem.removesuffix("_processed"))
