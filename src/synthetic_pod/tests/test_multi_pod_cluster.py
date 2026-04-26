from __future__ import annotations

import sys
import unittest
from pathlib import Path

SYNTHETIC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNTHETIC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNTHETIC_ROOT))

import pod2_sim


class MultiPodClusterTests(unittest.TestCase):
    def test_default_configuration_keeps_single_pod_behavior(self) -> None:
        args = pod2_sim.parse_args([])
        self.assertEqual(pod2_sim.resolve_pod_ids(args), ["02"])

    def test_cluster_mode_expands_nine_sequential_pods(self) -> None:
        args = pod2_sim.parse_args(["--pod-count", "9", "--pod-id-start", "02"])
        self.assertEqual(
            pod2_sim.resolve_pod_ids(args),
            ["02", "03", "04", "05", "06", "07", "08", "09", "10"],
        )

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
