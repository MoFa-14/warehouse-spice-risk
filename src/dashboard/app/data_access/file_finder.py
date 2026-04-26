"""File-discovery helpers for dashboard CSV fallback access.

The dashboard prefers SQLite when it is available, but the project still keeps
CSV-oriented utilities for older exports, offline debugging, and compatibility
with earlier phases of the repository. This module is the path-discovery layer
for those file-based readers.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from app.data_access.sqlite_reader import discover_pod_ids_from_sqlite, sqlite_db_exists


def discover_pod_ids(data_root: Path, *, db_path: Path | None = None) -> list[str]:
    """Discover pod identifiers from every storage surface the dashboard knows.

    A pod should remain visible once it has contributed stored data, even if it
    is no longer connected. For that reason discovery now considers SQLite
    telemetry, link history, forecasts, evaluations, and legacy folder names.
    """
    roots = [Path(data_root) / "raw" / "pods", Path(data_root) / "processed" / "pods"]
    pod_ids: set[str] = set()
    candidate_db_path = Path(db_path) if db_path is not None else Path(data_root) / "db" / "telemetry.sqlite"
    if sqlite_db_exists(candidate_db_path):
        pod_ids.update(discover_pod_ids_from_sqlite(candidate_db_path))
    for root in roots:
        if not root.exists():
            continue
        pod_ids.update(path.name for path in root.iterdir() if path.is_dir())
    return sorted(pod_ids, key=_pod_id_sort_key)


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
    """Collect daily CSV files whose stems encode a usable date.

    The dashboard stores telemetry history in one-file-per-day form for the CSV
    fallback path. This helper keeps that convention centralised so the rest of
    the code can ask for "files in this day range" without repeating filename
    parsing logic.
    """
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


def _pod_id_sort_key(value: str) -> tuple[int, int | str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)
