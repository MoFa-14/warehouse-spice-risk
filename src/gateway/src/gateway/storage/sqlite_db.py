"""SQLite connection helpers, pragmas, and schema initialization."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from gateway.storage.paths import build_storage_paths


SAMPLES_RAW_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS samples_raw (
        ts_pc_utc TEXT NOT NULL,
        pod_id TEXT NOT NULL,
        session_id INTEGER NOT NULL DEFAULT 0,
        seq INTEGER NOT NULL,
        ts_uptime_s REAL,
        temp_c REAL,
        rh_pct REAL,
        flags INTEGER,
        rssi INTEGER,
        quality_flags TEXT,
        source TEXT,
        PRIMARY KEY (pod_id, session_id, seq)
    )
"""

SAMPLES_RAW_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_samples_time ON samples_raw(ts_pc_utc)",
    "CREATE INDEX IF NOT EXISTS idx_samples_pod_time ON samples_raw(pod_id, ts_pc_utc)",
)

OTHER_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS link_quality (
        ts_pc_utc TEXT NOT NULL,
        pod_id TEXT NOT NULL,
        connected INTEGER,
        last_rssi INTEGER,
        total_received INTEGER,
        total_missing INTEGER,
        total_duplicates INTEGER,
        disconnect_count INTEGER,
        reconnect_count INTEGER,
        missing_rate REAL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_link_time ON link_quality(ts_pc_utc)",
    "CREATE INDEX IF NOT EXISTS idx_link_pod_time ON link_quality(pod_id, ts_pc_utc)",
    """
    CREATE TABLE IF NOT EXISTS gateway_events (
        ts_pc_utc TEXT NOT NULL,
        level TEXT NOT NULL,
        pod_id TEXT,
        message TEXT NOT NULL
    )
    """,
)


def resolve_db_path(path: Path | str | None = None) -> Path:
    """Resolve the telemetry database path, defaulting to data/db/telemetry.sqlite."""
    if path is None:
        return build_storage_paths().telemetry_db_path()
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return build_storage_paths().root.parent / candidate


def connect_sqlite(db_path: Path | str | None = None, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a SQLite connection with the gateway pragmas applied."""
    resolved_path = resolve_db_path(db_path)
    if not readonly:
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

    if readonly:
        uri = f"file:{resolved_path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    else:
        connection = sqlite3.connect(resolved_path, timeout=5.0)

    connection.row_factory = sqlite3.Row
    _apply_pragmas(connection, readonly=readonly)
    return connection


def init_db(db_path: Path | str | None = None) -> Path:
    """Create the telemetry database file and initialize the schema."""
    resolved_path = resolve_db_path(db_path)
    connection = connect_sqlite(resolved_path)
    try:
        initialize_schema(connection)
    finally:
        connection.close()
    return resolved_path


def initialize_schema(connection: sqlite3.Connection) -> None:
    """Create tables and indexes if they do not already exist."""
    _ensure_samples_raw_schema(connection)
    _normalize_backfill_session_ids(connection)
    for statement in OTHER_SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()


def _apply_pragmas(connection: sqlite3.Connection, *, readonly: bool) -> None:
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")

    if readonly:
        for pragma in ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"):
            try:
                connection.execute(pragma)
            except sqlite3.OperationalError:
                continue
        return

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")


def _ensure_samples_raw_schema(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "samples_raw"):
        connection.execute(SAMPLES_RAW_TABLE_SQL)
        for statement in SAMPLES_RAW_INDEXES:
            connection.execute(statement)
        return

    columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(samples_raw)").fetchall()
    }
    if "session_id" in columns:
        for statement in SAMPLES_RAW_INDEXES:
            connection.execute(statement)
        return

    connection.execute(
        """
        CREATE TABLE samples_raw_v2 (
            ts_pc_utc TEXT NOT NULL,
            pod_id TEXT NOT NULL,
            session_id INTEGER NOT NULL DEFAULT 0,
            seq INTEGER NOT NULL,
            ts_uptime_s REAL,
            temp_c REAL,
            rh_pct REAL,
            flags INTEGER,
            rssi INTEGER,
            quality_flags TEXT,
            source TEXT,
            PRIMARY KEY (pod_id, session_id, seq)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO samples_raw_v2 (
            ts_pc_utc, pod_id, session_id, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
        )
        SELECT ts_pc_utc, pod_id, 0, seq, ts_uptime_s, temp_c, rh_pct, flags, rssi, quality_flags, source
        FROM samples_raw
        ORDER BY pod_id ASC, ts_pc_utc ASC, seq ASC
        """
    )
    connection.execute("DROP TABLE samples_raw")
    connection.execute("ALTER TABLE samples_raw_v2 RENAME TO samples_raw")
    for statement in SAMPLES_RAW_INDEXES:
        connection.execute(statement)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (str(table_name),),
    ).fetchone()
    return row is not None


def _normalize_backfill_session_ids(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "samples_raw"):
        return

    pod_rows = connection.execute(
        """
        SELECT pod_id, session_id, MIN(ts_pc_utc) AS first_ts
        FROM samples_raw
        WHERE source = 'CSV_BACKFILL' AND session_id >= 0
        GROUP BY pod_id, session_id
        ORDER BY pod_id ASC, first_ts ASC, session_id ASC
        """
    ).fetchall()
    if not pod_rows:
        return

    by_pod: dict[str, list[int]] = {}
    for row in pod_rows:
        by_pod.setdefault(str(row["pod_id"]), []).append(int(row["session_id"]))

    for pod_id, session_ids in by_pod.items():
        current_min = connection.execute(
            "SELECT COALESCE(MIN(session_id), 0) AS minimum_session_id FROM samples_raw WHERE pod_id = ?",
            (pod_id,),
        ).fetchone()
        minimum_session_id = int(current_min["minimum_session_id"]) if current_min is not None else 0
        next_negative_session_id = min(minimum_session_id, 0) - len(session_ids)
        for offset, old_session_id in enumerate(session_ids):
            new_session_id = next_negative_session_id + offset
            connection.execute(
                """
                UPDATE samples_raw
                SET session_id = ?
                WHERE pod_id = ? AND source = 'CSV_BACKFILL' AND session_id = ?
                """,
                (new_session_id, pod_id, old_session_id),
            )
