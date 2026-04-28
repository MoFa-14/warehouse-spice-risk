# File overview:
# - Responsibility: Provides regression coverage for multi pod cluster behavior.
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

import pod2_sim
# Class purpose: Groups related regression checks for MultiPodCluster behavior.
# - Project role: Belongs to the test and regression coverage and groups related
#   state or behavior behind one explicit interface.
# - Inputs: Initialization parameters and later method calls defined on the class.
# - Outputs: Instances that hold state and expose related methods for later calls.
# - Important decisions: Historical fixes and future refactors both depend on this
#   coverage staying explicit.
# - Related flow: Calls runtime helpers or routes and asserts expected outcomes.

class MultiPodClusterTests(unittest.TestCase):
    # Test purpose: Verifies that default configuration keeps single pod
    #   behavior behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiPodClusterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_default_configuration_keeps_single_pod_behavior(self) -> None:
        args = pod2_sim.parse_args([])
        self.assertEqual(pod2_sim.resolve_pod_ids(args), ["02"])
    # Test purpose: Verifies that cluster mode expands nine sequential pods
    #   behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiPodClusterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_cluster_mode_expands_nine_sequential_pods(self) -> None:
        args = pod2_sim.parse_args(["--pod-count", "9", "--pod-id-start", "02"])
        self.assertEqual(
            pod2_sim.resolve_pod_ids(args),
            ["02", "03", "04", "05", "06", "07", "08", "09", "10"],
        )
    # Test purpose: Verifies that seed base assigns deterministic unique per pod
    #   seeds behaves as expected under this regression scenario.
    # - Project role: Belongs to the test and regression coverage and acts as a
    #   method on MultiPodClusterTests.
    # - Inputs: No explicit arguments beyond module or instance context.
    # - Outputs: No direct return value; failures surface through assertions.
    # - Important decisions: Keeps one concrete regression scenario executable
    #   so later refactors can be checked automatically.
    # - Related flow: Executes runtime code under a controlled scenario and
    #   checks the expected branch, value, or data contract.

    def test_seed_base_assigns_deterministic_unique_per_pod_seeds(self) -> None:
        args = pod2_sim.parse_args(["--pod-count", "3", "--pod-id-start", "02", "--seed-base", "100"])
        pod_ids = pod2_sim.resolve_pod_ids(args)
        seeds = [
            pod2_sim.build_pod_args(args, pod_id=pod_id, index=index).seed
            for index, pod_id in enumerate(pod_ids)
        ]
        self.assertEqual(seeds, [100, 101, 102])


if __name__ == "__main__":
    unittest.main()
