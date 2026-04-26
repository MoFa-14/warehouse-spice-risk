from __future__ import annotations

import sys
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.cli.forecast_cli import cli, parse_args
from gateway.forecast.runner import ForecastRunner
from gateway.logging.process_lock import GatewayProcessLock, build_lock_path


class ForecastCliTests(unittest.TestCase):
    def test_run_command_defaults_to_five_minute_cadence(self) -> None:
        args = parse_args(["run", "--all"])
        self.assertEqual(args.every_minutes, 5)

    def test_sqlite_forecast_lock_is_separate_from_gateway_db_lock(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            db_path = data_root / "db" / "telemetry.sqlite"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    """
                    CREATE TABLE samples_raw (
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
            runner = ForecastRunner(storage_backend="sqlite", data_root=data_root, db_path=db_path)
            self.assertEqual(
                build_lock_path(runner.lock_target),
                data_root / "db" / "forecast_runner.sqlite.lock",
            )
            self.assertNotEqual(
                build_lock_path(runner.lock_target),
                build_lock_path(db_path),
            )

    def test_cli_refuses_to_start_when_forecast_lock_is_already_held(self) -> None:
        with TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            (data_root / "raw" / "pods" / "01").mkdir(parents=True, exist_ok=True)
            lock = GatewayProcessLock(build_lock_path(data_root / "ml" / "forecast_runner.sqlite"))
            lock.acquire()
            try:
                result = cli(
                    [
                        "once",
                        "--all",
                        "--storage",
                        "csv",
                        "--data-root",
                        str(data_root),
                    ]
                )
            finally:
                lock.release()

            self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
