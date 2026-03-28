"""Plausible telemetry generation for the synthetic pod."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class SyntheticTelemetryGenerator:
    """Generate realistic-looking warehouse telemetry with slow drift."""

    pod_id: str
    interval_s: int
    seq: int = 0
    uptime_s: float = 0.0
    temp_c: float = 24.0
    rh_pct: float = 42.0
    temp_drift: float = 0.0
    rh_drift: float = 0.0

    def next_sample(self) -> dict[str, object]:
        self.seq += 1
        self.uptime_s += float(self.interval_s)

        self.temp_drift = self._clamp(self.temp_drift + random.uniform(-0.02, 0.02), -0.08, 0.08)
        self.rh_drift = self._clamp(self.rh_drift + random.uniform(-0.10, 0.10), -0.40, 0.40)

        self.temp_c = self._clamp(self.temp_c + self.temp_drift + random.uniform(-0.25, 0.25), 20.0, 29.0)
        self.rh_pct = self._clamp(self.rh_pct + self.rh_drift + random.uniform(-1.5, 1.5), 28.0, 70.0)

        if random.random() < 0.08:
            self.rh_pct = self._clamp(self.rh_pct + random.uniform(8.0, 14.0), 28.0, 72.0)

        return {
            "pod_id": self.pod_id,
            "seq": self.seq,
            "ts_uptime_s": round(self.uptime_s, 1),
            "temp_c": round(self.temp_c, 3),
            "rh_pct": round(self.rh_pct, 3),
            "flags": 0,
        }

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))
