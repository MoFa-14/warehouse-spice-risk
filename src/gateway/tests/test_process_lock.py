# File overview:
# - Responsibility: Provides regression coverage for process lock behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gateway.logging import process_lock
from gateway.logging.process_lock import GatewayProcessLock, ProcessStatus, build_lock_path
# Class purpose: Groups related regression checks for ProcessLock behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class ProcessLockTests(unittest.TestCase):
    # Test purpose: Verifies that build lock path targets exact SQLite file
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ProcessLockTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_build_lock_path_targets_exact_sqlite_file(self) -> None:
        self.assertEqual(
            build_lock_path(Path("data/db/telemetry.sqlite")),
            Path("data/db/telemetry.sqlite.lock"),
        )
        self.assertEqual(build_lock_path(Path("gateway/logs")), Path("gateway/logs/.lock"))
    # Test purpose: Verifies that windows probe error is treated as not running
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ProcessLockTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_windows_probe_error_is_treated_as_not_running(self) -> None:
        with (
            mock.patch.object(process_lock.os, "name", "nt"),
            mock.patch.object(process_lock, "_get_process_status_windows", side_effect=OSError(87, "bad pid")),
        ):
            self.assertFalse(process_lock._process_is_running(34380))
    # Test purpose: Verifies that acquire overwrites stale lock when process is
    #   not running behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ProcessLockTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_acquire_overwrites_stale_lock_when_process_is_not_running(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / ".lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "pid": 34380,
                        "created_at_utc": "2026-03-28T14:37:57Z",
                    }
                ),
                encoding="utf-8",
            )

            lock = GatewayProcessLock(lock_path)
            with (
                mock.patch.object(process_lock.os, "name", "nt"),
                mock.patch.object(
                    process_lock,
                    "_get_process_status_windows",
                    return_value=ProcessStatus(pid=34380, is_running=False, start_time_utc=None),
                ),
                mock.patch.object(process_lock.os, "getpid", return_value=777),
                mock.patch.object(process_lock, "_get_process_start_time_utc", return_value=None),
            ):
                lock.acquire()

            payload = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["pid"], 777)
            lock.release()
            self.assertFalse(lock_path.exists())
    # Test purpose: Verifies that acquire overwrites stale lock when pid is
    #   reused by other process behaves as expected under this regression
    #   scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ProcessLockTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_acquire_overwrites_stale_lock_when_pid_is_reused_by_other_process(self) -> None:
        with TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / ".lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "pid": 20664,
                        "created_at_utc": "2026-03-28T15:15:22Z",
                        "process_start_utc": "2026-03-28T15:15:22Z",
                    }
                ),
                encoding="utf-8",
            )

            lock = GatewayProcessLock(lock_path)
            with (
                mock.patch.object(process_lock.os, "name", "nt"),
                mock.patch.object(
                    process_lock,
                    "_get_process_status_windows",
                    return_value=ProcessStatus(
                        pid=20664,
                        is_running=True,
                        start_time_utc=process_lock.parse_utc_iso("2026-03-28T15:40:00Z"),
                    ),
                ),
                mock.patch.object(process_lock.os, "getpid", return_value=888),
                mock.patch.object(process_lock, "_get_process_start_time_utc", return_value=None),
            ):
                lock.acquire()

            payload = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["pid"], 888)
            lock.release()
    # Test purpose: Verifies that active legacy lock blocks new SQLite file lock
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on ProcessLockTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_active_legacy_lock_blocks_new_sqlite_file_lock(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_dir = Path(temp_dir)
            legacy_lock_path = db_dir / ".lock"
            new_lock_path = db_dir / "telemetry.sqlite.lock"
            legacy_lock_path.write_text(
                json.dumps(
                    {
                        "pid": 20664,
                        "created_at_utc": "2026-03-28T15:15:22Z",
                        "process_start_utc": "2026-03-28T15:15:22Z",
                        "argv": ["python", "-m", "gateway.main"],
                    }
                ),
                encoding="utf-8",
            )

            lock = GatewayProcessLock(new_lock_path, compatibility_lock_paths=(legacy_lock_path,))
            with (
                mock.patch.object(process_lock.os, "name", "nt"),
                mock.patch.object(
                    process_lock,
                    "_get_process_status_windows",
                    return_value=ProcessStatus(
                        pid=20664,
                        is_running=True,
                        start_time_utc=process_lock.parse_utc_iso("2026-03-28T15:15:22Z"),
                    ),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "pid=20664"):
                    lock.acquire()

            self.assertFalse(new_lock_path.exists())


if __name__ == "__main__":
    unittest.main()
