"""Single-process lock file for the gateway log directory."""

from __future__ import annotations

import ctypes
import contextlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from gateway.utils.timeutils import parse_utc_iso, utc_now_iso


_WINDOWS_SYNCHRONIZE = 0x00100000
_WINDOWS_QUERY_LIMITED_INFORMATION = 0x1000
_WAIT_TIMEOUT = 0x00000102


class _FileTime(ctypes.Structure):
    _fields_ = [("dwLowDateTime", ctypes.c_ulong), ("dwHighDateTime", ctypes.c_ulong)]


@dataclass(frozen=True)
class ProcessStatus:
    """Minimal process metadata used to validate lock ownership."""

    pid: int
    is_running: bool
    start_time_utc: datetime | None


@dataclass
class GatewayProcessLock:
    """Prevent multiple gateway instances from writing the same log directory."""

    lock_path: Path
    compatibility_lock_paths: tuple[Path, ...] = ()
    acquired: bool = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        process_start = _get_process_start_time_utc(os.getpid())
        payload = {
            "pid": os.getpid(),
            "created_at_utc": utc_now_iso(),
            "process_start_utc": utc_now_iso(process_start) if process_start is not None else None,
            "argv": sys.argv,
        }
        while True:
            _verify_or_clear_compatibility_locks(self.compatibility_lock_paths)
            try:
                _write_lock_payload_exclusive(self.lock_path, payload)
                self.acquired = True
                return
            except FileExistsError:
                existing_payload = _read_lock_payload(self.lock_path)
                existing_pid = _lock_payload_pid(existing_payload)
                if existing_pid is not None and _lock_owner_is_active(existing_payload):
                    raise RuntimeError(_lock_conflict_message(self.lock_path, existing_pid, existing_payload))
                _remove_stale_lock(self.lock_path)

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        finally:
            self.acquired = False


def _read_lock_payload(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def build_lock_path(target: Path | str) -> Path:
    """Return the lock-file path for a directory or a specific SQLite file."""
    resolved = Path(target)
    if resolved.suffix:
        return resolved.with_name(f"{resolved.name}.lock")
    return resolved / ".lock"


def _write_lock_payload_exclusive(path: Path, payload: dict[str, object]) -> None:
    fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        raise


def _remove_stale_lock(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _verify_or_clear_compatibility_locks(paths: tuple[Path, ...]) -> None:
    for path in paths:
        if not path.exists():
            continue
        payload = _read_lock_payload(path)
        existing_pid = _lock_payload_pid(payload)
        if existing_pid is not None and _lock_owner_is_active(payload):
            raise RuntimeError(_lock_conflict_message(path, existing_pid, payload))
        _remove_stale_lock(path)


def _lock_payload_pid(payload: dict[str, object] | None) -> int | None:
    if payload is None:
        return None
    pid = payload.get("pid")
    if isinstance(pid, int):
        return pid
    return None


def _process_is_running(pid: int) -> bool:
    status = _get_process_status(pid)
    return status.is_running


def _get_process_status(pid: int) -> ProcessStatus:
    if pid <= 0:
        return ProcessStatus(pid=pid, is_running=False, start_time_utc=None)

    if os.name == "nt":
        try:
            return _get_process_status_windows(pid)
        except OSError:
            return ProcessStatus(pid=pid, is_running=False, start_time_utc=None)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return ProcessStatus(pid=pid, is_running=False, start_time_utc=None)
    except PermissionError:
        return ProcessStatus(pid=pid, is_running=True, start_time_utc=None)
    except OSError:
        return ProcessStatus(pid=pid, is_running=False, start_time_utc=None)
    return ProcessStatus(pid=pid, is_running=True, start_time_utc=None)


def _get_process_status_windows(pid: int) -> ProcessStatus:
    access = _WINDOWS_SYNCHRONIZE | _WINDOWS_QUERY_LIMITED_INFORMATION
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(access, False, pid)
    if not handle:
        error_code = ctypes.get_last_error()
        return ProcessStatus(pid=pid, is_running=(error_code == 5), start_time_utc=None)

    try:
        wait_result = kernel32.WaitForSingleObject(handle, 0)
        is_running = wait_result == _WAIT_TIMEOUT
        return ProcessStatus(
            pid=pid,
            is_running=is_running,
            start_time_utc=_get_process_start_time_utc_windows_handle(handle),
        )
    finally:
        kernel32.CloseHandle(handle)


def _get_process_start_time_utc(pid: int) -> datetime | None:
    return _get_process_status(pid).start_time_utc


def _get_process_start_time_utc_windows_handle(handle) -> datetime | None:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    creation_time = _FileTime()
    exit_time = _FileTime()
    kernel_time = _FileTime()
    user_time = _FileTime()
    success = kernel32.GetProcessTimes(
        handle,
        ctypes.byref(creation_time),
        ctypes.byref(exit_time),
        ctypes.byref(kernel_time),
        ctypes.byref(user_time),
    )
    if not success:
        return None
    return _filetime_to_datetime(creation_time)


def _filetime_to_datetime(filetime: _FileTime) -> datetime | None:
    raw_value = (int(filetime.dwHighDateTime) << 32) | int(filetime.dwLowDateTime)
    if raw_value <= 0:
        return None
    unix_epoch_offset = 116444736000000000
    microseconds = (raw_value - unix_epoch_offset) // 10
    return datetime.fromtimestamp(microseconds / 1_000_000, tz=timezone.utc)


def _lock_owner_is_active(payload: dict[str, object] | None) -> bool:
    pid = _lock_payload_pid(payload)
    if pid is None:
        return False

    status = _get_process_status(pid)
    if not status.is_running:
        return False

    expected_start = _parse_payload_time(payload, "process_start_utc")
    if expected_start is None:
        expected_start = _parse_payload_time(payload, "created_at_utc")

    if expected_start is None or status.start_time_utc is None:
        return True

    return abs(status.start_time_utc - expected_start) <= timedelta(seconds=10)


def _parse_payload_time(payload: dict[str, object] | None, key: str) -> datetime | None:
    if payload is None:
        return None
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return parse_utc_iso(value)
    except ValueError:
        return None


def _lock_conflict_message(lock_path: Path, pid: int, payload: dict[str, object] | None) -> str:
    created_at = _parse_payload_time(payload, "created_at_utc")
    argv = payload.get("argv") if isinstance(payload, dict) else None
    details = []
    if created_at is not None:
        details.append(f"started_at={utc_now_iso(created_at)}")
    if isinstance(argv, list) and argv:
        details.append(f"argv={' '.join(str(part) for part in argv)}")

    suffix = f" ({', '.join(details)})" if details else ""
    return f"Another gateway process (pid={pid}) is already writing to {lock_path}{suffix}."
