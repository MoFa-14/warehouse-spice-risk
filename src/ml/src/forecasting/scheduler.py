# File overview:
# - Responsibility: Simple recurring scheduler used by the forecast CLI.
# - Project role: Defines feature extraction, analogue matching, scenario
#   generation, evaluation, and forecasting utilities.
# - Main data or concerns: Feature vectors, trajectories, event labels, metrics, and
#   model configuration.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.
# - Why this matters: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.

"""Simple recurring scheduler used by the forecast CLI."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from forecasting.utils import floor_to_interval
# Class purpose: Run a callback immediately and then on a fixed minute cadence.
# - Project role: Belongs to the forecast model and evaluation layer and groups
#   related state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: The forecast pipeline depends on these modules to keep the
#   predictive transformation path explicit.
# - Related flow: Consumes forecast-ready telemetry windows and passes trajectories
#   or metrics to gateway orchestration.

@dataclass
class ForecastScheduler:
    """Run a callback immediately and then on a fixed minute cadence."""

    every_minutes: int
    duration_s: float | None = None
    align_to_wall_clock: bool = False
    sleep_step_s: float = 1.0
    # Method purpose: Implements the run step used by this subsystem.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on ForecastScheduler.
    # - Inputs: Arguments such as callback, interpreted according to the rules
    #   encoded in the body below.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: The forecast pipeline depends on these modules to
    #   keep the predictive transformation path explicit.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or metrics to gateway orchestration.

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
    # Method purpose: Implements the initial run time step used by this
    #   subsystem.
    # - Project role: Belongs to the forecast model and evaluation layer and
    #   acts as a method on ForecastScheduler.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns datetime when the function completes successfully.
    # - Important decisions: The forecast pipeline depends on these modules to
    #   keep the predictive transformation path explicit.
    # - Related flow: Consumes forecast-ready telemetry windows and passes
    #   trajectories or metrics to gateway orchestration.

    def _initial_run_time(self) -> datetime:
        now = datetime.now(timezone.utc)
        if self.align_to_wall_clock:
            return floor_to_interval(now, self.every_minutes)
        return now
