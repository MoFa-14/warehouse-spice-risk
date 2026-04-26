from __future__ import annotations

import sys
import unittest
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.faults import FaultController, FaultProfile


class _BurstRng:
    def __init__(self) -> None:
        self.random_values = [0.0, 1.0, 1.0, 1.0, 1.0]

    def random(self) -> float:
        if self.random_values:
            return self.random_values.pop(0)
        return 1.0

    def uniform(self, lower: float, upper: float) -> float:
        return upper


class BurstLossTests(unittest.TestCase):
    def test_burst_mode_increases_drop_and_delay_probability(self) -> None:
        controller = FaultController(
            profile=FaultProfile(
                p_drop=0.10,
                p_delay=0.20,
                p_corrupt=0.0,
                p_disconnect=0.0,
                burst_loss_enabled=True,
                burst_duration_s=30.0,
                burst_multiplier=4.0,
                burst_trigger_probability=1.0,
            ),
            interval_s=10.0,
            rng=_BurstRng(),
        )

        action = controller.choose_action(disturbance_active=True)

        self.assertTrue(action.burst_active)
        self.assertAlmostEqual(action.effective_p_drop, 0.40)
        self.assertAlmostEqual(action.effective_p_delay, 0.80)
        self.assertGreater(controller.burst_remaining_s, 0.0)


if __name__ == "__main__":
    unittest.main()
