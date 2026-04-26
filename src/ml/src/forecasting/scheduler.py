"""Simple recurring scheduler used by the forecast CLI."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from forecasting.utils import floor_to_interval


@dataclass
class ForecastScheduler:
    """Run a callback immediately and then on a fixed minute cadence."""

    every_minutes: int
    duration_s: float | None = None
    align_to_wall_clock: bool = False
    sleep_step_s: float = 1.0

    def run(self, callback: Callable[[datetime], None]) -> None:
        start_monotonic = time.monotonic()
        next_run = self._initial_run_time()

        while True:
            now = datetime.now(timezone.utc)
            if now >= next_run:
                callback(next_run)
                next_run = next_run + timedelta(minutes=self.every_minutes)

            if self.duration_s is not None and (time.monotonic() - start_monotonic) >= self.duration_s:
                return
            time.sleep(self.sleep_step_s)

    def _initial_run_time(self) -> datetime:
        now = datetime.now(timezone.utc)
        if self.align_to_wall_clock:
            return floor_to_interval(now, self.every_minutes)
        return now
