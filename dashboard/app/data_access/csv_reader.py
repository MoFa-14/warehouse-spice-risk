"""Pandas CSV readers shared by dashboard services."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


RAW_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "seq",
    "ts_uptime_s",
    "temp_c",
    "rh_pct",
    "flags",
    "rssi",
    "quality_flags",
]

PROCESSED_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "temp_c_clean",
    "rh_pct_clean",
    "dew_point_c",
    "missing",
    "interpolated",
    "source_seq",
]

LINK_COLUMNS = [
    "ts_pc_utc",
    "pod_id",
    "connected",
    "last_rssi",
    "total_received",
    "total_missing",
    "total_duplicates",
    "disconnect_count",
    "reconnect_count",
    "missing_rate",
]


def read_raw_samples(paths: Iterable[Path]) -> pd.DataFrame:
    """Read raw telemetry CSV files into one normalized dataframe."""
    return _read_csvs(
        paths,
        RAW_COLUMNS,
        numeric_columns=["seq", "ts_uptime_s", "temp_c", "rh_pct", "flags", "rssi", "quality_flags"],
    )


def read_processed_samples(paths: Iterable[Path]) -> pd.DataFrame:
    """Read processed daily CSV files into one normalized dataframe."""
    return _read_csvs(
        paths,
        PROCESSED_COLUMNS,
        numeric_columns=["temp_c_clean", "rh_pct_clean", "dew_point_c", "missing", "interpolated", "source_seq"],
    )


def read_link_quality(paths: Iterable[Path]) -> pd.DataFrame:
    """Read link-quality CSV files into one normalized dataframe."""
    return _read_csvs(
        paths,
        LINK_COLUMNS,
        numeric_columns=[
            "connected",
            "last_rssi",
            "total_received",
            "total_missing",
            "total_duplicates",
            "disconnect_count",
            "reconnect_count",
            "missing_rate",
        ],
    )


def _read_csvs(paths: Iterable[Path], columns: list[str], *, numeric_columns: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not Path(path).exists() or Path(path).stat().st_size == 0:
            continue
        frames.append(pd.read_csv(path))

    if not frames:
        return pd.DataFrame(columns=columns)

    dataframe = pd.concat(frames, ignore_index=True)
    for column in columns:
        if column not in dataframe.columns:
            dataframe[column] = pd.NA
    dataframe = dataframe[columns].copy()
    dataframe["ts_pc_utc"] = pd.to_datetime(dataframe["ts_pc_utc"], utc=True, errors="coerce")
    dataframe = dataframe.dropna(subset=["ts_pc_utc"]).sort_values("ts_pc_utc").reset_index(drop=True)
    dataframe["pod_id"] = dataframe["pod_id"].astype(str)
    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    return dataframe
