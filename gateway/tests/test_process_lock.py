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


class ProcessLockTests(unittest.TestCase):
    def test_build_lock_path_targets_exact_sqlite_file(self) -> None:
        self.assertEqual(
            build_lock_path(Path("data/db/telemetry.sqlite")),
            Path("data/db/telemetry.sqlite.lock"),
        )
        self.assertEqual(build_lock_path(Path("gateway/logs")), Path("gateway/logs/.lock"))

    def test_windows_probe_error_is_treated_as_not_running(self) -> None:
        with (
            mock.patch.object(process_lock.os, "name", "nt"),
            mock.patch.object(process_lock, "_get_process_status_windows", side_effect=OSError(87, "bad pid")),
        ):
            self.assertFalse(process_lock._process_is_running(34380))

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
