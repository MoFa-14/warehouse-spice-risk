"""Pandas CSV readers shared by dashboard services."""

from __future__ import annotations

import math
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
    "dew_point_c",
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
    dataframe = _read_csvs(
        paths,
        RAW_COLUMNS,
        numeric_columns=["seq", "ts_uptime_s", "temp_c", "rh_pct", "dew_point_c", "flags", "rssi", "quality_flags"],
    )
    return _fill_missing_raw_dew_point(dataframe)


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
    normalized_paths = [Path(path) for path in paths]
    frames: list[pd.DataFrame] = []
    for file_order, path in enumerate(normalized_paths):
        if not path.exists() or path.stat().st_size == 0:
            continue
        frame = pd.read_csv(path, converters={"pod_id": _read_pod_id})
        frame["__file_order"] = file_order
        frame["__row_order"] = range(len(frame))
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=columns)

    dataframe = pd.concat(frames, ignore_index=True)
    for column in columns:
        if column not in dataframe.columns:
            dataframe[column] = pd.NA

    ordered_columns = ["__file_order", "__row_order", *columns]
    dataframe = dataframe[ordered_columns].copy()
    dataframe["ts_pc_utc"] = pd.to_datetime(dataframe["ts_pc_utc"], utc=True, errors="coerce")
    dataframe = (
        dataframe.dropna(subset=["ts_pc_utc"])
        .sort_values(["ts_pc_utc", "__file_order", "__row_order"], kind="mergesort")
        .reset_index(drop=True)
    )
    dataframe["pod_id"] = dataframe["pod_id"].astype("string").fillna("").astype(str)
    dataframe = dataframe[columns].copy()
    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    return dataframe


def _fill_missing_raw_dew_point(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "dew_point_c" not in dataframe.columns:
        return dataframe

    missing_mask = dataframe["dew_point_c"].isna()
    if not missing_mask.any():
        return dataframe

    dataframe = dataframe.copy()
    dataframe.loc[missing_mask, "dew_point_c"] = dataframe.loc[missing_mask].apply(
        lambda row: _dew_point_c(row.get("temp_c"), row.get("rh_pct")),
        axis=1,
    )
    return dataframe


def _read_pod_id(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _dew_point_c(temp_c, rh_pct):
    if pd.isna(temp_c) or pd.isna(rh_pct):
        return float("nan")
    rh = max(1e-6, min(float(rh_pct), 100.0)) / 100.0
    a, b = 17.62, 243.12
    gamma = (a * float(temp_c) / (b + float(temp_c))) + math.log(rh)
    return (b * gamma) / (a - gamma)
