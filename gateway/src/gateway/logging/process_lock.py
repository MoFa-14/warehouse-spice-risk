"""Single-process lock file for the gateway log directory."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from gateway.utils.timeutils import utc_now_iso


@dataclass
class GatewayProcessLock:
    """Prevent multiple gateway instances from writing the same log directory."""

    lock_path: Path
    acquired: bool = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if self.lock_path.exists():
            existing_pid = _read_lock_pid(self.lock_path)
            if existing_pid is not None and _process_is_running(existing_pid):
                raise RuntimeError(
                    f"Another gateway process (pid={existing_pid}) is already writing to {self.lock_path.parent}."
                )

        payload = {
            "pid": os.getpid(),
            "created_at_utc": utc_now_iso(),
        }
        self.lock_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self.acquired = True

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        finally:
            self.acquired = False


def _read_lock_pid(path: Path) -> int | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    pid = payload.get("pid")
    if isinstance(pid, int):
        return pid
    return None


def _process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
