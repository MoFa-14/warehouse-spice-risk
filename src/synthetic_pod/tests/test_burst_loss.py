# File overview:
# - Responsibility: Provides regression coverage for burst loss behavior.
# - Project role: Keeps runtime behavior executable and checkable through automated
#   scenarios.
# - Main data or concerns: Fixture data, expected outputs, and regression scenarios.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.
# - Why this matters: Historical fixes and future refactors both depend on this
#   coverage staying explicit.

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

from sim.faults import FaultController, FaultProfile
# Class purpose: Encapsulates the BurstRng responsibilities used by this module.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class _BurstRng:
    # Method purpose: Initializes object state and attaches the dependencies or
    #   values needed by later methods.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _BurstRng.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; the function performs state updates or
    #   side effects.
    # - Important decisions: Initialization must make dependencies and default
    #   state explicit because later methods assume that setup has completed
    #   correctly.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def __init__(self) -> None:
        self.random_values = [0.0, 1.0, 1.0, 1.0, 1.0]
    # Method purpose: Implements the random step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _BurstRng.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def random(self) -> float:
        if self.random_values:
            return self.random_values.pop(0)
        return 1.0
    # Method purpose: Implements the uniform step used by this subsystem.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on _BurstRng.
    # - Inputs: Arguments such as lower, upper, interpreted according to the
    #   rules encoded in the body below.
    # - Outputs: Returns float when the function completes successfully.
    # - Important decisions: Historical fixes and future refactors both depend
    #   on this coverage staying explicit.
    # - Related flow: Calls runtime helpers or routes and asserts expected
    #   outcomes.

    def uniform(self, lower: float, upper: float) -> float:
        return upper
# Class purpose: Groups related regression checks for BurstLoss behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class BurstLossTests(unittest.TestCase):
    # Test purpose: Verifies that burst mode increases drop and delay
    #   probability behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on BurstLossTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

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
